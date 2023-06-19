from localstack.extensions.api import Extension, http, aws
from localstack.http import Response
from localstack.aws.chain import HandlerChain
from localstack.aws.api import RequestContext

from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


class MyExtension(Extension):
    name = "opentelemetry-extension"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = None

    def on_extension_load(self):
        print("opentelemetry: extension is loaded")

    def on_platform_start(self):
        print("opentelemetry: localstack is starting")

    def on_platform_ready(self):
        print("opentelemetry: localstack is running")

        exporter = ConsoleSpanExporter()
        resource = Resource(
            attributes={
                SERVICE_NAME: "localstack",
            }
        )
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(__name__)

    def update_request_handlers(self, handlers: aws.CompositeHandler):
        handlers.append(self.add_request_tracing)

    def update_response_handlers(self, handlers: aws.CompositeResponseHandler):
        handlers.append(self.finish_request_tracing)

    def add_request_tracing(self, chain: HandlerChain, ctx: RequestContext, response: Response):
        if not self.tracer:
            return
        # set up span
        # TODO: handle if headers already present
        if not ctx.request:
            return

        tctx = propagate.extract(ctx.request.headers)
        print(f"Previous context: {tctx} {type(tctx)=}")
        extra_headers = {}
        with self.tracer.start_as_current_span("request", context=tctx):
            propagate.inject(extra_headers)

        print(f"injecting headers: {extra_headers}")
        ctx.request.headers.update(**extra_headers)

    def finish_request_tracing(self, chain: HandlerChain, ctx: RequestContext, response: Response):
        if not self.tracer:
            return

        print("post-processing request")
        # TODO: close span
