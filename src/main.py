import src.instrumentation  # must be first

import os
import logging
import time
import random
import threading
from datetime import datetime
from typing import Optional, List

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from opentelemetry import trace, metrics
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

logger = logging.getLogger("claims-service")

app = FastAPI(title="InsureWatch Claims Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
FastAPIInstrumentor.instrument_app(app)

# MongoDB
MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = AsyncIOMotorClient(MONGO_URL)
db = client.insurewatch
claims_collection = db.claims

# OTel instruments
tracer = trace.get_tracer("claims-service", "1.0.0")
meter  = metrics.get_meter("claims-service", "1.0.0")

claims_submitted  = meter.create_counter("claims.submitted.total",  description="Total claims submitted")
claims_approved   = meter.create_counter("claims.approved.total",   description="Total claims approved")
claims_rejected   = meter.create_counter("claims.rejected.total",   description="Total claims rejected")
processing_time   = meter.create_histogram("claims.processing.duration", unit="ms", description="Claim processing time")
active_claims     = meter.create_up_down_counter("claims.active", description="Currently active claims")

# ── Chaos state ───────────────────────────────────────────────────────────────
chaos_state = {
    "service_crash":   False,
    "high_latency":    False,
    "db_failure":      False,
    "memory_spike":    False,
    "cpu_spike":       False,
}
_memory_hog: List = []

def apply_chaos():
    """Apply chaos effects based on current state"""
    if chaos_state["service_crash"]:
        raise HTTPException(status_code=503, detail="Service unavailable (chaos: service_crash)")

    if chaos_state["high_latency"]:
        delay = random.uniform(3.0, 8.0)
        logger.warning(f"Chaos: injecting {delay:.1f}s latency")
        time.sleep(delay)

    if chaos_state["db_failure"]:
        raise HTTPException(status_code=503, detail="Database connection failed (chaos: db_failure)")

    if chaos_state["memory_spike"]:
        logger.warning("Chaos: memory spike - allocating 50MB")
        _memory_hog.append("x" * 50 * 1024 * 1024)

    if chaos_state["cpu_spike"]:
        logger.warning("Chaos: CPU spike starting")
        end = time.time() + 2
        while time.time() < end:
            _ = sum(i * i for i in range(10000))

# ── Models ────────────────────────────────────────────────────────────────────
class ClaimSubmission(BaseModel):
    customer_id: str
    policy_number: str
    claim_type: str  # medical, auto, property, life
    amount: float
    description: str
    incident_date: str

class ClaimResponse(BaseModel):
    claim_id: str
    customer_id: str
    policy_number: str
    claim_type: str
    amount: float
    status: str
    submitted_at: str
    description: str

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "service": "claims-service", "chaos": chaos_state}

@app.post("/claims", response_model=ClaimResponse)
async def submit_claim(claim: ClaimSubmission, request: Request):
    start = time.time()
    with tracer.start_as_current_span("submit_claim") as span:
        apply_chaos()

        span.set_attribute("claim.customer_id",   claim.customer_id)
        span.set_attribute("claim.type",          claim.claim_type)
        span.set_attribute("claim.amount",        claim.amount)
        span.set_attribute("claim.policy_number", claim.policy_number)

        logger.info(f"Processing claim submission for customer {claim.customer_id}")

        # Validate policy with Policy Service
        policy_url = os.getenv("POLICY_SERVICE_URL", "http://localhost:8080")
        try:
            async with httpx.AsyncClient(timeout=8.0) as http:
                policy_resp = await http.get(f"{policy_url}/policy/{claim.customer_id}/coverage")
                if policy_resp.status_code != 200:
                    raise HTTPException(status_code=400, detail="Policy not found or inactive")
                policy_data = policy_resp.json()
                span.set_attribute("policy.valid", True)
                span.set_attribute("policy.coverage_limit", policy_data.get("coverage_limit", 0))
        except httpx.RequestError as e:
            logger.error(f"Policy service unreachable: {e}")
            span.record_exception(e)
            span.set_attribute("policy.valid", False)
            # Continue with claim but flag it
            logger.warning("Proceeding with claim despite policy service being unreachable")

        # Determine claim status
        status = "pending"
        if claim.amount < 1000:
            status = "auto_approved"
            claims_approved.add(1, {"claim_type": claim.claim_type})
        else:
            claims_submitted.add(1, {"claim_type": claim.claim_type})

        # Persist to MongoDB
        doc = {
            "customer_id":   claim.customer_id,
            "policy_number": claim.policy_number,
            "claim_type":    claim.claim_type,
            "amount":        claim.amount,
            "description":   claim.description,
            "incident_date": claim.incident_date,
            "status":        status,
            "submitted_at":  datetime.utcnow().isoformat(),
        }
        result = await claims_collection.insert_one(doc)
        claim_id = str(result.inserted_id)
        span.set_attribute("claim.id",     claim_id)
        span.set_attribute("claim.status", status)
        active_claims.add(1)

        # Notify notification service async
        notification_url = os.getenv("NOTIFICATION_SERVICE_URL", "http://localhost:3003")
        try:
            async with httpx.AsyncClient(timeout=3.0) as http:
                await http.post(f"{notification_url}/notify", json={
                    "customer_id": claim.customer_id,
                    "event":       "claim_submitted",
                    "claim_id":    claim_id,
                    "status":      status,
                })
        except Exception as e:
            logger.warning(f"Notification service unreachable (non-fatal): {e}")

        duration_ms = (time.time() - start) * 1000
        processing_time.record(duration_ms, {"claim_type": claim.claim_type, "status": status})
        logger.info(f"Claim {claim_id} created with status {status}", extra={"claim_id": claim_id})

        return ClaimResponse(
            claim_id=claim_id,
            customer_id=claim.customer_id,
            policy_number=claim.policy_number,
            claim_type=claim.claim_type,
            amount=claim.amount,
            status=status,
            submitted_at=doc["submitted_at"],
            description=claim.description,
        )

@app.get("/claims/{claim_id}")
async def get_claim(claim_id: str):
    with tracer.start_as_current_span("get_claim") as span:
        apply_chaos()
        span.set_attribute("claim.id", claim_id)
        from bson import ObjectId
        try:
            doc = await claims_collection.find_one({"_id": ObjectId(claim_id)})
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid claim ID")
        if not doc:
            raise HTTPException(status_code=404, detail="Claim not found")
        doc["claim_id"] = str(doc.pop("_id"))
        return doc

@app.get("/claims")
async def list_claims(customer_id: Optional[str] = None):
    with tracer.start_as_current_span("list_claims") as span:
        apply_chaos()
        query = {}
        if customer_id:
            query["customer_id"] = customer_id
            span.set_attribute("filter.customer_id", customer_id)
        cursor = claims_collection.find(query).limit(50)
        results = []
        async for doc in cursor:
            doc["claim_id"] = str(doc.pop("_id"))
            results.append(doc)
        span.set_attribute("claims.count", len(results))
        return results

# ── Chaos endpoints ───────────────────────────────────────────────────────────
@app.get("/chaos/state")
async def get_chaos_state():
    return chaos_state

@app.post("/chaos/set")
async def set_chaos(state: dict):
    global _memory_hog
    for key, value in state.items():
        if key in chaos_state:
            chaos_state[key] = value
            logger.warning(f"Chaos state updated: {key}={value}")
    if not chaos_state["memory_spike"]:
        _memory_hog = []  # Release memory when disabled
    return {"status": "updated", "chaos": chaos_state}
