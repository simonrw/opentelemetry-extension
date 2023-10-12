from __future__ import annotations

import logging
from collections import defaultdict
from unittest.mock import patch

from localstack import config
from localstack.aws.api import RequestContext
from localstack.aws.chain import Handler, HandlerChain
from localstack.aws.connect import InternalClientFactory
from localstack.extensions.api import Extension, aws
from localstack.http import Response
from opentelemetry import trace, propagate
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    BatchSpanProcessor,
)
from opentelemetry.trace import set_span_in_context

LOG = logging.getLogger(__name__)

"""
https://opentelemetry.io/docs/instrumentation/python/cookbook/#manually-setting-span-context
"""


class OpenTelemetryExtension(Extension):
    name = "opentelemetry-extension"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = None
        self.span_map = {}
        self.depths = defaultdict(int)

    def on_extension_load(self):
        logging.getLogger(__name__).setLevel(
            level=logging.DEBUG if config.DEBUG else logging.INFO
        )
        LOG.info("opentelemetry: extension is loaded")

        BotocoreInstrumentor().instrument()

        # self.patch_client_creation()

    # def patch_client_creation(self):
    #     patch("localstack.aws.connect.InternalClientFactory.get_client",
    #           self.instrumented_get_client(InternalClientFactory.get_client))
    #
    # def instrumented_get_client(self, original_method):
    #     def inner(*args, **kwargs):
    #         client = original_method(*args, **kwargs)
    #         LOG.warning("OVERRIDDEN GET_CLIENT")
    #         return client
    #
    #     return inner
    #
    def on_platform_start(self):
        LOG.info("opentelemetry: localstack is starting")

    def on_platform_ready(self):
        LOG.info("opentelemetry: localstack is running")

        resource = Resource(attributes={SERVICE_NAME: "localstack"})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)

        self.tracer = trace.get_tracer("my.tracer")
        self.propagator = propagate.get_global_textmap()

    def update_request_handlers(self, handlers: aws.CompositeHandler):
        handlers.append(self.log_request_headers("request-handler"))
        handlers.append(self.log_response_headers("request-handler"))
        handlers.append(self.add_request_tracing)

    def update_response_handlers(self, handlers: aws.CompositeResponseHandler):
        handlers.append(self.log_request_headers("response-handler"))
        handlers.append(self.log_response_headers("response-handler"))
        handlers.append(self.finish_request_tracing)

    def log_request_headers(self, name: str) -> Handler:
        def inner(chain: HandlerChain, ctx: RequestContext, response: Response):
            LOG.warning("*** REQUEST: %s %r", name, ctx.request.headers)

        return inner

    def log_response_headers(self, name: str) -> Handler:
        def inner(chain: HandlerChain, ctx: RequestContext, response: Response):
            LOG.warning("*** RESPONSE: %s %r", name, response.headers)

        return inner

    def add_request_tracing(
        self, chain: HandlerChain, ctx: RequestContext, response: Response
    ):
        if not self.tracer:
            return
        if not ctx.request:
            return
        if not ctx.service:
            return
        if not ctx.operation:
            return

        name = f"{ctx.service}.{ctx.operation}"

        headers = ctx.request.headers
        trace_context = self.propagator.extract(headers)
        if trace_context:
            LOG.warning("request: trace context found")
            # child span
            span = self.tracer.start_span(name, trace_context)
        else:
            LOG.warning("request: no trace context found, creating root span")
            # root span
            span = self.tracer.start_span("root", trace_context)

        span.set_attributes(
            {
                "service": ctx.service.service_name,
                "operation": ctx.operation.name,
            }
        )

        new_context = set_span_in_context(span, trace_context)
        self.propagator.inject(headers, context=new_context)
        self.span_map[headers["traceparent"]] = (span, trace_context)
        # NEEDED?
        ctx.request.headers = headers
        # forward the traceparent to the response so we can recover this span on the way back
        response.headers["traceparent"] = headers["traceparent"]

    def finish_request_tracing(
        self, chain: HandlerChain, ctx: RequestContext, response: Response
    ):
        if not self.tracer:
            return

        if not ctx.request:
            return

        trace_context = self.propagator.extract(response.headers)

        if not trace_context:
            LOG.error("response: no trace context found")
            return

        LOG.warning("response: trace context found")

        traceparent = response.headers.get("traceparent")
        res = self.span_map.pop(traceparent, None)
        if not res:
            return

        span, old_context = res
        span.end()

        # inject the old context back into the response for propagation
        self.propagator.inject(response.headers, old_context)
