from __future__ import annotations

import logging

from collections import defaultdict
from localstack import config
from localstack.extensions.api import Extension, http, aws
from localstack.http import Response
from localstack.aws.chain import HandlerChain
from localstack.aws.api import RequestContext

from opentelemetry import trace, propagate
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import Span, set_span_in_context
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

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
        logging.getLogger(__name__).setLevel(level=logging.DEBUG if config.DEBUG else logging.INFO)
        LOG.info("opentelemetry: extension is loaded")

    def on_platform_start(self):
        LOG.info("opentelemetry: localstack is starting")

    def on_platform_ready(self):
        LOG.info("opentelemetry: localstack is running")

        resource = Resource(attributes={SERVICE_NAME: "localstack"})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
        provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)

        self.tracer = trace.get_tracer("my.tracer")
        self.propagator = propagate.get_global_textmap()

    def update_request_handlers(self, handlers: aws.CompositeHandler):
        handlers.append(self.add_request_tracing)

    def update_response_handlers(self, handlers: aws.CompositeResponseHandler):
        handlers.append(self.finish_request_tracing)

    def add_request_tracing(self, chain: HandlerChain, ctx: RequestContext, response: Response):
        if not self.tracer:
            return
        if not ctx.request:
            return

        LOG.debug("adding context")

        headers = ctx.request.headers
        trace_context = self.propagator.extract(headers)
        name = f"{ctx.service}.{ctx.operation}"
        if trace_context:
            # child span
            span = self.tracer.start_span(name, trace_context)
        else:
            # root span
            span = self.tracer.start_span("root", trace_context)

        new_context = set_span_in_context(span)
        self.propagator.inject(headers, context=new_context)
        self.span_map[headers["traceparent"]] = (span, trace_context)
        # NEEDED?
        ctx.request.headers = headers

    def finish_request_tracing(self, chain: HandlerChain, ctx: RequestContext, response: Response):
        if not self.tracer:
            return

        if not ctx.request:
            return

        headers = ctx.request.headers
        trace_context = self.propagator.extract(headers)
        if not trace_context:
            return

        LOG.warning("trace context found")

        res = self.span_map.pop(headers["traceparent"], None)
        if not res:
            return

        span, old_context = res
        span.end()

        self.propagator.inject(headers, old_context)
        # NEEDED?
        response.headers = headers
