/** Token usage event — matches spec/schema/token-event-v1.json */

export interface TokenEvent {
  eventId?: string;
  customerId: string;
  modelId: string;
  provider?: string;
  inputTokens?: number;
  outputTokens?: number;
  cacheReadTokens?: number;
  cacheWriteTokens?: number;
  reasoningTokens?: number;
  embeddingTokens?: number;
  requestId?: string;
  spanId?: string;
  parentSpanId?: string;
  sessionId?: string;
  timestamp?: number;
  latencyMs?: number;
  environment?: string;
  metadata?: Record<string, string>;
}

export function totalTokens(e: TokenEvent): number {
  return (
    (e.inputTokens ?? 0) +
    (e.outputTokens ?? 0) +
    (e.cacheReadTokens ?? 0) +
    (e.cacheWriteTokens ?? 0) +
    (e.reasoningTokens ?? 0) +
    (e.embeddingTokens ?? 0)
  );
}

export function toEventPayload(event: TokenEvent): Record<string, unknown> {
  const d: Record<string, unknown> = {
    customerId: event.customerId,
    modelId: event.modelId,
    provider: event.provider ?? "openai",
    inputTokens: event.inputTokens ?? 0,
    outputTokens: event.outputTokens ?? 0,
    cacheReadTokens: event.cacheReadTokens ?? 0,
    cacheWriteTokens: event.cacheWriteTokens ?? 0,
    reasoningTokens: event.reasoningTokens ?? 0,
    embeddingTokens: event.embeddingTokens ?? 0,
    timestamp: event.timestamp ?? Date.now(),
    latencyMs: event.latencyMs ?? 0,
  };
  if (event.eventId) d.eventId = event.eventId;
  if (event.requestId) d.requestId = event.requestId;
  if (event.spanId) d.spanId = event.spanId;
  if (event.parentSpanId) d.parentSpanId = event.parentSpanId;
  if (event.sessionId) d.sessionId = event.sessionId;
  if (event.environment) d.environment = event.environment;
  if (event.metadata) d.metadata = event.metadata;
  return d;
}
