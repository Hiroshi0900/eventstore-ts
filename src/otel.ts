import { context, propagation } from "@opentelemetry/api";

export interface TraceContext {
  readonly traceParent: string;
  readonly traceState: string;
}

/**
 * Extracts W3C trace context from the active OTel context, matching the Go
 * store's otel.GetTextMapPropagator().Inject into a map carrier. Returns
 * empty strings when no propagator/span is configured.
 */
export function currentTraceContext(): TraceContext {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return {
    traceParent: carrier.traceparent ?? "",
    traceState: carrier.tracestate ?? "",
  };
}
