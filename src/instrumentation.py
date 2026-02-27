"""
instrumentation.py - Initialize OpenTelemetry before anything else
"""
import os
import logging
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor

OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
OTLP_HEADERS = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")

# Parse headers from comma-separated format: "key1=value1,key2=value2"
headers = {}
if OTLP_HEADERS:
    for header in OTLP_HEADERS.split(','):
        if '=' in header:
            key, value = header.split('=', 1)
            headers[key.strip()] = value.strip()

resource = Resource.create({
    "service.name": "claims-service",
    "service.version": "1.0.0",
    "deployment.environment": os.getenv("ENVIRONMENT", "production"),
    "service.language": "python",
})

# Traces
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTLP_ENDPOINT}/v1/traces", headers=headers))
)
trace.set_tracer_provider(tracer_provider)

# Metrics
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{OTLP_ENDPOINT}/v1/metrics", headers=headers),
    export_interval_millis=10000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# Logs
logger_provider = LoggerProvider(resource=resource)
set_logger_provider(logger_provider)
logger_provider.add_log_record_processor(
    BatchLogRecordProcessor(
        OTLPLogExporter(endpoint=f"{OTLP_ENDPOINT}/v1/logs", headers=headers)
    )
)

# Auto-instrumentations
PymongoInstrumentor().instrument()
HTTPXClientInstrumentor().instrument()
LoggingInstrumentor().instrument(set_logging_format=True)

# Inject traceId/spanId into Python logs and bridge to OTel
handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] [traceId=%(otelTraceID)s spanId=%(otelSpanID)s] %(message)s",
)
