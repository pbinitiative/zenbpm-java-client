package org.zenbpm.rest;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

public final class ZenbpmOkHttpOtelInterceptor implements Interceptor {

    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;

    public ZenbpmOkHttpOtelInterceptor(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("org.zenbpm.rest");
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();

        String method = request.method();
        String url = request.url().toString();

        Span span = tracer.spanBuilder(method + " " + request.url().encodedPath())
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        span.setAttribute("http.request.method", method);
        span.setAttribute("url.full", url);

        Request.Builder builder = request.newBuilder();
        openTelemetry.getPropagators().getTextMapPropagator().inject(
                io.opentelemetry.context.Context.current().with(span),
                builder,
                (b, key, value) -> b.header(key, value)
        );

        try (Scope scope = span.makeCurrent()) {
            Response response = chain.proceed(builder.build());
            int code = response.code();
            span.setAttribute("http.response.status_code", code);
            if (code >= 400) {
                span.setStatus(StatusCode.ERROR);
            }
            return response;
        } catch (IOException | RuntimeException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }
}
