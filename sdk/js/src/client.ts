import { randomUUID } from "node:crypto";
import { TokenEvent, toEventPayload } from "./event.js";

export interface FluxMeterOptions {
  /** HTTP ingest base URL (default http://localhost:8000) */
  apiUrl?: string;
  apiKey?: string;
  environment?: string;
  maxRetries?: number;
  retryBaseMs?: number;
}

export class DeliveryError extends Error {
  constructor(
    public readonly eventId: string,
    message: string,
  ) {
    super(message);
    this.name = "DeliveryError";
  }
}

type OpenAIUsage = {
  prompt_tokens?: number;
  completion_tokens?: number;
  prompt_tokens_details?: { cached_tokens?: number };
  completion_tokens_details?: { reasoning_tokens?: number };
};

type OpenAIResponse = {
  id?: string;
  model: string;
  usage: OpenAIUsage;
};

type AnthropicUsage = {
  input_tokens?: number;
  output_tokens?: number;
  cache_read_input_tokens?: number;
  cache_creation_input_tokens?: number;
};

type AnthropicResponse = {
  id?: string;
  model: string;
  usage: AnthropicUsage;
};

type TrackOpts = {
  sessionId?: string;
  spanId?: string;
  latencyMs?: number;
  environment?: string;
};

function parseOpenAIUsage(response: OpenAIResponse) {
  const usage = response.usage;
  return {
    modelId: response.model,
    inputTokens: usage.prompt_tokens ?? 0,
    outputTokens: usage.completion_tokens ?? 0,
    cacheReadTokens: usage.prompt_tokens_details?.cached_tokens ?? 0,
    reasoningTokens: usage.completion_tokens_details?.reasoning_tokens ?? 0,
    requestId: response.id,
  };
}

export class FluxMeter {
  private apiUrl: string;
  private apiKey?: string;
  private environment?: string;
  private maxRetries: number;
  private retryBaseMs: number;

  constructor(options: FluxMeterOptions = {}) {
    this.apiUrl = (options.apiUrl ?? "http://localhost:8000").replace(/\/$/, "");
    this.apiKey = options.apiKey;
    this.environment = options.environment;
    this.maxRetries = Math.max(0, options.maxRetries ?? 2);
    this.retryBaseMs = Math.max(0, options.retryBaseMs ?? 100);
  }

  /** Compatibility no-op: HTTP requests transfer custody synchronously. */
  async close(): Promise<void> {
    return;
  }

  async track(
    customerId: string,
    modelId: string,
    fields: Partial<Omit<TokenEvent, "customerId" | "modelId">> = {},
  ): Promise<TokenEvent> {
    const event: TokenEvent = {
      customerId,
      modelId,
      provider: fields.provider ?? "openai",
      inputTokens: fields.inputTokens ?? 0,
      outputTokens: fields.outputTokens ?? 0,
      cacheReadTokens: fields.cacheReadTokens ?? 0,
      cacheWriteTokens: fields.cacheWriteTokens ?? 0,
      reasoningTokens: fields.reasoningTokens ?? 0,
      embeddingTokens: fields.embeddingTokens ?? 0,
      eventId: fields.eventId ?? randomUUID(),
      requestId: fields.requestId,
      spanId: fields.spanId,
      parentSpanId: fields.parentSpanId,
      sessionId: fields.sessionId,
      latencyMs: fields.latencyMs ?? 0,
      environment: fields.environment ?? this.environment,
      metadata: fields.metadata,
      timestamp: fields.timestamp ?? Date.now(),
    };
    await this.send(event);
    return event;
  }

  async trackOpenAI(
    customerId: string,
    response: OpenAIResponse,
    opts: TrackOpts = {},
  ): Promise<TokenEvent> {
    const parsed = parseOpenAIUsage(response);
    return this.track(customerId, parsed.modelId, {
      provider: "openai",
      inputTokens: parsed.inputTokens,
      outputTokens: parsed.outputTokens,
      cacheReadTokens: parsed.cacheReadTokens,
      reasoningTokens: parsed.reasoningTokens,
      requestId: parsed.requestId,
      sessionId: opts.sessionId,
      spanId: opts.spanId,
      latencyMs: opts.latencyMs ?? 0,
      environment: opts.environment,
    });
  }

  private async trackOpenAICompatible(
    customerId: string,
    response: OpenAIResponse,
    provider: string,
    opts: TrackOpts = {},
  ): Promise<TokenEvent> {
    const parsed = parseOpenAIUsage(response);
    return this.track(customerId, parsed.modelId, {
      provider,
      inputTokens: parsed.inputTokens,
      outputTokens: parsed.outputTokens,
      cacheReadTokens: parsed.cacheReadTokens,
      reasoningTokens: parsed.reasoningTokens,
      requestId: parsed.requestId,
      sessionId: opts.sessionId,
      spanId: opts.spanId,
      latencyMs: opts.latencyMs ?? 0,
      environment: opts.environment,
    });
  }

  async trackDeepSeek(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "deepseek", opts);
  }

  async trackQwen(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "qwen", opts);
  }

  async trackGLM(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "zhipu", opts);
  }

  async trackMoonshot(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "moonshot", opts);
  }

  async trackDoubao(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "doubao", opts);
  }

  async trackBaichuan(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "baichuan", opts);
  }

  async trackMiniMax(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "minimax", opts);
  }

  async trackHunyuan(customerId: string, response: OpenAIResponse, opts: TrackOpts = {}) {
    return this.trackOpenAICompatible(customerId, response, "hunyuan", opts);
  }

  async trackAnthropic(
    customerId: string,
    response: AnthropicResponse,
    opts: TrackOpts = {},
  ): Promise<TokenEvent> {
    const usage = response.usage;
    return this.track(customerId, response.model, {
      provider: "anthropic",
      inputTokens: usage.input_tokens ?? 0,
      outputTokens: usage.output_tokens ?? 0,
      cacheReadTokens: usage.cache_read_input_tokens ?? 0,
      cacheWriteTokens: usage.cache_creation_input_tokens ?? 0,
      requestId: response.id,
      sessionId: opts.sessionId,
      spanId: opts.spanId,
      latencyMs: opts.latencyMs ?? 0,
      environment: opts.environment,
    });
  }

  private async send(event: TokenEvent): Promise<void> {
    const payload = toEventPayload(event);
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey) headers["X-API-Key"] = this.apiKey;

    let lastError: unknown;
    for (let attempt = 0; attempt <= this.maxRetries; attempt += 1) {
      try {
        const res = await fetch(`${this.apiUrl}/ingest`, {
          method: "POST",
          headers,
          body: JSON.stringify(payload),
        });
        if (!res.ok) {
          const message = `HTTP ${res.status}: ${await res.text()}`;
          if (res.status !== 429 && res.status !== 503 && res.status < 500) {
            throw new DeliveryError(event.eventId ?? "unknown", message);
          }
          throw new Error(message);
        }
        return;
      } catch (error) {
        if (error instanceof DeliveryError) throw error;
        lastError = error;
        if (attempt < this.maxRetries) {
          await new Promise((resolve) =>
            setTimeout(resolve, this.retryBaseMs * 2 ** attempt),
          );
        }
      }
    }
    throw new DeliveryError(
      event.eventId ?? "unknown",
      `FluxMeter delivery failed: ${String(lastError)}`,
    );
  }
}
