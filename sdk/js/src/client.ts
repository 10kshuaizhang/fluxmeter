import { randomUUID } from "node:crypto";
import { TokenEvent, toEventPayload } from "./event.js";

export interface FluxMeterOptions {
  /** HTTP ingest base URL (default http://localhost:8000) */
  apiUrl?: string;
  /** Kafka brokers — if set, uses kafkajs instead of HTTP */
  kafkaBrokers?: string;
  topic?: string;
  apiKey?: string;
  environment?: string;
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
  private topic: string;
  private apiKey?: string;
  private environment?: string;
  private kafkaBrokers?: string;
  private producer: {
    send: (topic: string, messages: { key: string; value: string }[]) => Promise<void>;
    disconnect: () => Promise<void>;
  } | null = null;
  private producerReady: Promise<void> | null = null;

  constructor(options: FluxMeterOptions = {}) {
    this.apiUrl = (options.apiUrl ?? "http://localhost:8000").replace(/\/$/, "");
    this.topic = options.topic ?? "token-events";
    this.apiKey = options.apiKey;
    this.environment = options.environment;
    this.kafkaBrokers = options.kafkaBrokers;
  }

  private ensureKafka(): Promise<void> {
    if (!this.kafkaBrokers) {
      return Promise.resolve();
    }
    if (!this.producerReady) {
      this.producerReady = this.initKafka(this.kafkaBrokers);
    }
    return this.producerReady;
  }

  private async initKafka(brokers: string): Promise<void> {
    const { Kafka } = await import("kafkajs");
    const kafka = new Kafka({ brokers: brokers.split(",") });
    const producer = kafka.producer();
    await producer.connect();
    this.producer = {
      send: async (topic, messages) => {
        await producer.send({
          topic,
          messages: messages.map((m) => ({
            key: m.key,
            value: m.value,
          })),
        });
      },
      disconnect: () => producer.disconnect(),
    };
  }

  /** Release Kafka producer connections (no-op for HTTP transport). */
  async close(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
      this.producerReady = null;
    }
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

    await this.ensureKafka();
    if (this.producer) {
      await this.producer.send(this.topic, [
        {
          key: event.customerId,
          value: JSON.stringify(payload),
        },
      ]);
      return;
    }

    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey) headers["X-API-Key"] = this.apiKey;

    const res = await fetch(`${this.apiUrl}/ingest`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      throw new Error(`FluxMeter ingest failed: ${res.status} ${await res.text()}`);
    }
  }
}
