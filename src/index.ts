import { Request, Response, NextFunction } from "express";
import { trace as otelTrace } from "@opentelemetry/api";
import { TraceMonitor, TraceSpan } from "./trace";
import { randomUUID as uuid } from "crypto";

import { diagnostics } from "./diagnostics";
import { onModuleLoad } from "./module-hook";

/**
 * Field names whose values are replaced with [REDACTED] before anything leaves
 * the process. Matched as case-insensitive substrings, against request bodies
 * AND request headers - "cookie" also covers "set-cookie", "authorization"
 * covers the Authorization header, "apiKey" covers "x-api-key".
 */
const SENSITIVE_FIELDS = [
  "password",
  "token",
  "secret",
  "apikey",
  "api-key",
  "authorization",
  "creditcard",
  "ssn",
  "cookie",
];
import { AGENT_VERSION } from "./version";

// Custom DB instrumentations
import { InstrumentationManager } from "./instrumentations/instrumentation-manager";
import {
  MongoInstrumentation,
  MongoSdkSpan,
} from "./instrumentations/mongo.instrumentation";
import {
  RedisInstrumentation,
  RedisSdkSpan,
} from "./instrumentations/redis.instrumentation";
import {
  PostgresInstrumentation,
  PostgresSdkSpan,
} from "./instrumentations/postgres.instrumentation";

// Host (CPU / memory / event-loop) metrics
import {
  HostMetricsMonitor,
  HostMetricSample,
} from "./metrics/host-metrics-monitor";

export interface PolarTraceConfig {
  apiKey: string;
  serviceName: string;
  endpoint?: string;
  /**
   * Mirror the agent's own diagnostics to stderr. Errors and warnings are
   * always reported there; this adds the informational and debug detail.
   */
  enableConsoleLog?: boolean;
  /**
   * Append the agent's own diagnostics to this file. No file is created
   * unless a path is given.
   */
  logFile?: string;
  captureHeaders?: boolean;
  captureBody?: boolean;
  captureQuery?: boolean;
  captureConsoleLogs?: boolean;
  /**
   * Enable custom MongoDB + Mongoose span collection.
   * When true, the agent will patch mongodb/mongoose and emit Mongo spans
   * via the same pipeline as other OpenTelemetry spans.
   */
  enableMongoSpanCollection?: boolean;
  /**
   * Enable custom Redis span collection (ioredis + node-redis v4+).
   * When true (the default), the agent patches both drivers and emits Redis
   * command spans via the same pipeline as Mongo / HTTP spans, so apps that
   * use Redis and MongoDB together produce a single coherent trace.
   */
  enableRedisSpanCollection?: boolean;
  /**
   * Enable custom PostgreSQL span collection (`pg` driver).
   * When true (the default), the agent patches pg's Client.query (which also
   * covers Pool.query) and emits Postgres query spans via the same pipeline
   * as Mongo / Redis / HTTP spans.
   */
  enablePostgresSpanCollection?: boolean;
  /**
   * Enable host metrics collection (CPU, memory, event-loop lag).
   * When true (the default), the agent samples `process.*` / `os.*` / event-loop
   * delay every 10s and ships them to the collector alongside logs and traces.
   */
  enableHostMetrics?: boolean;
  /**
   * Optional mongoose instance from the host application.
   * Providing this ensures Mongo instrumentation patches the SAME mongoose
   * that the app uses, even when the agent is loaded from a different package.
   */
  mongoose?: any;
}

export interface RequestLog {
  timestamp: string;
  method: string;
  path: string;
  statusCode?: number;
  duration: number;
  startTime?: string;
  endTime?: string;
  headers?: Record<string, string | string[] | undefined>;
  query?: Record<string, any>;
  body?: any;
  userAgent?: string;
  ip?: string;
  host?: string;
  consoleLogs?: Array<{ timestamp: string; level: string; args: any[] }>;
  /**
   * OpenTelemetry trace id (32-char hex, converted to the 32-char UUID form
   * the collector stores on the traces table). Lets the UI join a request log
   * to its trace and show the captured console output inside the trace view.
   */
  traceId?: string;
  error?: {
    message: string;
    stack?: string;
    name?: string;
  };
}

export interface TraceSpanData extends TraceSpan {
  serviceVersion?: string;
}

interface QueuedLog {
  log: RequestLog;
}

interface QueuedTraceSpan {
  span: TraceSpanData;
}

interface QueuedHostMetric {
  sample: HostMetricSample;
}

type ConnectionStatus = "pending" | "connected" | "failed";

class PolarTrace {
  private config: Required<
    Omit<PolarTraceConfig, "endpoint" | "mongoose" | "logFile">
  > & {
    endpoint?: string;
    mongoose?: any;
    logFile?: string;
  };
  private connectionStatus: ConnectionStatus = "pending";
  private connectionError?: string;
  private statusAnnounced = false;
  private logQueue: QueuedLog[] = [];
  private flushTimer?: NodeJS.Timeout;
  private defaultHeaders: Record<string, string>;
  private originalConsoleLog?: typeof console.log;
  private originalConsoleError?: typeof console.error;
  private originalConsoleWarn?: typeof console.warn;
  private originalConsoleInfo?: typeof console.info;
  private isFlushing: boolean = false;
  private isTraceFlushing: boolean = false;
  private isHostMetricsFlushing: boolean = false;
  private traceMonitor?: TraceMonitor;
  private traceQueue: QueuedTraceSpan[] = [];
  private traceFlushTimer?: NodeJS.Timeout;
  private hostMetricsMonitor?: HostMetricsMonitor;
  private readonly hostMetricsQueue: QueuedHostMetric[] = [];
  private hostMetricsFlushTimer?: NodeJS.Timeout;
  private readonly HOST_METRICS_QUEUE_LIMIT = 1000; // ~2.7 hours @ 10s sampling
  private readonly LOG_QUEUE_LIMIT = 5000; // drop-oldest bound, mirrors host metrics
  private readonly TRACE_QUEUE_LIMIT = 5000; // drop-oldest bound, mirrors host metrics
  private readonly MAX_BATCH_SIZE = 500; // max items shipped per flush tick
  private readonly FLUSH_INTERVAL = 10000; // 10 seconds
  private readonly API_TIMEOUT = 10000; // 10 seconds
  private readonly MAX_BACKOFF_MS = 300000; // 5 minutes - retry backoff ceiling

  // Retry backoff state, tracked per stream (logs / traces / host metrics).
  // While backoffUntil is in the future, flush ticks for that stream are
  // skipped; the deadline doubles on each consecutive retained failure.
  private logConsecutiveFailures = 0;
  private logBackoffUntil = 0;
  private traceConsecutiveFailures = 0;
  private traceBackoffUntil = 0;
  private hostMetricsConsecutiveFailures = 0;
  private hostMetricsBackoffUntil = 0;
  // Trace grouping: cache spans by traceId until parent span completes
  private traceCache: Map<string, TraceSpanData[]> = new Map();
  private parentSpanTracker: Map<string, string> = new Map(); // traceId -> parentSpanId
  private traceTimestamps: Map<string, number> = new Map(); // traceId -> creation timestamp
  private readonly TRACE_CACHE_TIMEOUT = 60000; // 60 seconds - cleanup orphaned traces

  // Shared span ID mapping: maps 16-character OpenTelemetry span IDs to 32-character UUIDs
  // This maintains parent-child relationships across trace-monitor and MongoDB spans
  private spanIdMap: Map<string, string> = new Map();

  constructor(config: PolarTraceConfig) {
    const defaultEndpoint = "https://collector.polartrace.io/api/log";

    this.config = {
      enableConsoleLog: false,
      captureHeaders: true,
      captureBody: true,
      captureQuery: true,
      captureConsoleLogs: true,
      enableMongoSpanCollection: true,
      enableRedisSpanCollection: true,
      enablePostgresSpanCollection: true,
      enableHostMetrics: true,
      endpoint: defaultEndpoint,
      ...config,
    };

    diagnostics.configure({
      verbose: this.config.enableConsoleLog,
      filePath: this.config.logFile,
    });

    // Validate serviceName
    if (!this.config.serviceName) {
      throw new Error(
        "serviceName is required (set POLARTRACE_APP_NAME)",
      );
    }

    this.defaultHeaders = {
      "Content-Type": "application/json",
      "internal-access-token": this.config.apiKey,
      "service-name": this.config.serviceName,
    };

    this.validateApiKey();
    this.setupFlushTimer();
    // We start it synchronously to ensure zero code changes are needed in user's app
    this.initializeTraceMonitoringSync();
    this.initializeHostMetricsMonitoring();
    // No continuous polling - validation only happens once when service starts
    this.validateConnection().catch((err) => {
      // Error handling is done in validateConnection
    });

    // Show status summary after initialization (defer to next tick to allow all init to complete)
    setImmediate(() => {
      this.showConnectionStatus();
    });
  }

  /**
   * POST JSON to an endpoint using native fetch with a timeout.
   * Throws on network errors, abort/timeout, and HTTP status >= 500.
   * Returns {status, data} for any status < 500 (callers handle 4xx).
   */
  private async postJson(
    url: string,
    body: any,
  ): Promise<{ status: number; data: any; retryAfterSeconds?: number }> {
    const controller = new AbortController();
    const timeoutId = setTimeout(
      () => controller.abort(),
      this.API_TIMEOUT,
    );

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: this.defaultHeaders,
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      const text = await res.text();
      let data: any = undefined;
      if (text) {
        try {
          data = JSON.parse(text);
        } catch {
          data = text;
        }
      }

      if (res.status >= 500) {
        const err: any = new Error(`API returned status ${res.status}`);
        err.response = { status: res.status, data };
        throw err;
      }

      // On 429 the collector may tell us when to come back - surface the
      // Retry-After header (seconds) so flush backoff can honor it.
      let retryAfterSeconds: number | undefined;
      if (res.status === 429) {
        const retryAfterHeader = res.headers.get("retry-after");
        if (retryAfterHeader) {
          const seconds = Number(retryAfterHeader);
          if (Number.isFinite(seconds) && seconds > 0) {
            retryAfterSeconds = seconds;
          }
        }
      }

      return { status: res.status, data, retryAfterSeconds };
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Whether a failed batch is worth retrying.
   * 429 (collector backpressure), 5xx and network errors (no status) are
   * transient - retain the batch and retry on a later tick. Any other 4xx
   * (400/401/402/413...) is a deterministic rejection: retrying the same
   * payload can never succeed, so the batch must be dropped or it blocks
   * the queue head forever and grows memory unbounded.
   */
  private shouldRetainBatch(status?: number): boolean {
    if (status === undefined) {
      return true; // network error / timeout - no response at all
    }
    return status === 429 || status >= 500;
  }

  /**
   * Exponential backoff for retained failures: 10s, 20s, 40s... capped at
   * MAX_BACKOFF_MS. When the collector sent a Retry-After (429), honor the
   * larger of the two.
   */
  private nextBackoffMs(
    consecutiveFailures: number,
    retryAfterSeconds?: number,
  ): number {
    const exponential = Math.min(
      this.FLUSH_INTERVAL * Math.pow(2, consecutiveFailures - 1),
      this.MAX_BACKOFF_MS,
    );
    return Math.max(exponential, (retryAfterSeconds ?? 0) * 1000);
  }

  /**
   * Extract base URL from endpoint configuration
   * Returns the base URL (protocol + host) without path
   */
  private getBaseUrl(): string {
    if (!this.config.endpoint) {
      return "";
    }

    const endpoint = this.config.endpoint.trim();

    try {
      const url = new URL(endpoint);
      return `${url.protocol}//${url.host}`;
    } catch {
      // If not a full URL, try to extract host from endpoint using regex
      const match = endpoint.match(/^(https?:\/\/[^\/]+)/);
      if (match) {
        return match[1];
      }
      // Fallback to default
      return "";
    }
  }

  private validateApiKey(): void {
    if (!this.config.apiKey) {
      throw new Error("apiKey is required (set POLARTRACE_LICENSE_KEY)");
    }

    if (this.config.apiKey.length < 10) {
      throw new Error(
        "apiKey does not look like a PolarTrace license key (set POLARTRACE_LICENSE_KEY)",
      );
    }
  }

  private setupFlushTimer(): void {
    // Set up flush timer to send logs every 10 seconds
    if (this.config.endpoint) {
      this.flushTimer = setInterval(() => {
        this.flushLogs();
      }, this.FLUSH_INTERVAL);
      this.flushTimer.unref();

      // Set up trace flush timer (independent process)
      this.traceFlushTimer = setInterval(() => {
        this.flushTraces();
      }, this.FLUSH_INTERVAL);
      this.traceFlushTimer.unref();

      // Set up host metrics flush timer (independent process)
      this.hostMetricsFlushTimer = setInterval(() => {
        this.flushHostMetrics();
      }, this.FLUSH_INTERVAL);
      this.hostMetricsFlushTimer.unref();
    }
  }

  /**
   * Start the host metrics monitor. Samples are placed on a queue and flushed
   * by the same 10s flush timer used for logs/traces. This is the agent's
   * counterpart to the SDK an APM vendor would ship - see also `polartrace-collector`'s
   * `host-metrics` ingest endpoint.
   */
  private initializeHostMetricsMonitoring(): void {
    if (!this.config.enableHostMetrics) {
      return;
    }

    try {
      this.hostMetricsMonitor = new HostMetricsMonitor({
        serviceName: this.config.serviceName,
        sampleIntervalMs: this.FLUSH_INTERVAL,
        onSample: (sample) => this.queueHostMetricSample(sample),
        onError: (err) => {
          diagnostics.debug(
            `Host metrics sampling error: ${err.message}`,
          );
        },
      });
      this.hostMetricsMonitor.initialize();

      diagnostics.debug(
        "Host metrics monitoring started (CPU/memory/event-loop)",
      );
    } catch (err: any) {
      // Don't throw - host metrics are optional and must never break the host app.
      diagnostics.warn(
        `Failed to initialize host metrics monitoring: ${err.message}`,
      );
    }
  }

  private queueHostMetricSample(sample: HostMetricSample): void {
    this.hostMetricsQueue.push({ sample });

    // Bound the queue so a long collector outage doesn't grow memory unbounded.
    if (this.hostMetricsQueue.length > this.HOST_METRICS_QUEUE_LIMIT) {
      const overflow =
        this.hostMetricsQueue.length - this.HOST_METRICS_QUEUE_LIMIT;
      this.hostMetricsQueue.splice(0, overflow);
    }
  }

  /**
   * Turn a low-level fetch / undici error into a single actionable sentence.
   * Node's native fetch surfaces transport errors as `TypeError: fetch failed`
   * with the real reason buried in `error.cause` - we unwrap it here so the
   * status table shows "Collector unreachable at … (ECONNREFUSED)" instead of
   * just "fetch failed".
   */
  private describeNetworkError(error: any): string {
    const baseUrl = this.getBaseUrl() || this.config.endpoint || "(unknown)";
    const cause = error?.cause;
    const code = cause?.code || cause?.errno;
    const detail = cause?.message || error?.message || "unknown error";

    if (code === "ECONNREFUSED") {
      return `Collector at ${baseUrl} refused connection - is it running?`;
    }
    if (code === "ENOTFOUND" || code === "EAI_AGAIN") {
      return `Cannot resolve collector host ${baseUrl} (${code})`;
    }
    if (code === "ETIMEDOUT") {
      return `Collector at ${baseUrl} timed out`;
    }
    if (code) {
      return `Cannot reach collector at ${baseUrl} (${code})`;
    }
    return `Cannot reach collector at ${baseUrl} - ${detail}`;
  }

  /**
   * Validate connection with the collector. Auth/config errors (4xx with a
   * response body) are reported immediately - they won't self-heal. Network
   * errors (collector not up yet, DNS hiccup, refused connection) get a few
   * background retries with backoff so a collector that starts a moment
   * later doesn't leave the agent stuck in a misleading "failed" state.
   */
  private async validateConnection(): Promise<void> {
    if (!this.config.endpoint) {
      this.connectionStatus = "failed";
      this.connectionError = "Endpoint not configured";
      this.showConnectionStatus();
      return;
    }

    const baseUrl = this.getBaseUrl();
    const validationEndpoint = `${baseUrl}/api/service/validate`;

    // 5 attempts at 0s, 2s, 4s, 8s, 16s ≈ 30s total window for the collector
    // to come up. Anything still failing after that is shown as an error.
    const backoffsMs = [0, 2000, 4000, 8000, 16000];

    for (let attempt = 0; attempt < backoffsMs.length; attempt++) {
      if (backoffsMs[attempt] > 0) {
        await new Promise((r) => setTimeout(r, backoffsMs[attempt]));
      }

      try {
        const response = await this.postJson(validationEndpoint, {});

        if (response.status >= 200 && response.status < 300) {
          this.connectionStatus = "connected";
          this.connectionError = undefined;
          diagnostics.debug("Connection validated successfully");
          this.showConnectionStatus();
          return;
        }

        // 4xx with a body - auth/config problem. Won't self-heal, stop here.
        this.connectionStatus = "failed";
        this.connectionError =
          response.data?.message || `API returned status ${response.status}`;
        this.showConnectionStatus();
        return;
      } catch (error: any) {
        if (error?.response) {
          // Same as the 4xx branch above, just surfaced as a throw.
          this.connectionStatus = "failed";
          this.connectionError =
            error.response.data?.message ||
            `API returned status ${error.response.status}`;
          this.showConnectionStatus();
          return;
        }

        // Network / timeout - retry unless this was the last attempt.
        const isLast = attempt === backoffsMs.length - 1;
        const reason =
          error?.name === "AbortError"
            ? `Collector at ${baseUrl} did not respond within ${
                this.API_TIMEOUT / 1000
              }s`
            : this.describeNetworkError(error);

        diagnostics.debug(
          `validate attempt ${attempt + 1}/${backoffsMs.length} failed - ${reason}`,
        );

        if (isLast) {
          this.connectionStatus = "failed";
          this.connectionError = reason;
          this.showConnectionStatus();
          return;
        }
      }
    }
  }

  /**
   * Report the agent's readiness once startup has settled.
   *
   * A healthy agent stays quiet - it only says so at debug level. A degraded
   * one reports each failing component on stderr as a single actionable line,
   * because silently collecting nothing is the worst outcome for the operator.
   * Called from several points in the validation flow, so it reports once.
   */
  private showConnectionStatus(): void {
    // Validation still in flight - nothing conclusive to report yet.
    if (this.connectionStatus === "pending" || this.statusAnnounced) {
      return;
    }

    const globals = globalThis as any;
    const components: Array<{ name: string; ok: boolean; reason?: string }> = [
      {
        name: "collector",
        ok: this.connectionStatus === "connected",
        reason: this.connectionError,
      },
      { name: "http/express tracing", ok: !!this.traceMonitor?.initialized },
      {
        name: "mongodb spans",
        ok:
          !this.config.enableMongoSpanCollection ||
          !!globals.__POLARTRACE_MONGO_INSTRUMENTATION_ENABLED__,
      },
      {
        name: "redis spans",
        ok:
          !this.config.enableRedisSpanCollection ||
          !!globals.__POLARTRACE_REDIS_INSTRUMENTATION_ENABLED__,
      },
      {
        name: "postgres spans",
        ok:
          !this.config.enablePostgresSpanCollection ||
          !!globals.__POLARTRACE_POSTGRES_INSTRUMENTATION_ENABLED__,
      },
    ];

    this.statusAnnounced = true;
    const failed = components.filter((component) => !component.ok);

    if (failed.length === 0) {
      diagnostics.info(
        `agent ready (v${AGENT_VERSION}, service "${this.config.serviceName}")`,
      );
      return;
    }

    for (const component of failed) {
      diagnostics.error(
        component.reason
          ? `${component.name} unavailable: ${component.reason}`
          : `${component.name} unavailable`,
      );
    }
  }

  /**
   * Initialize trace monitoring SYNCHRONOUSLY in constructor
   * This ensures OpenTelemetry SDK starts immediately, before mongoose is required
   * This allows users to use PolarTrace without any code changes
   */
  private initializeTraceMonitoringSync(): void {
    try {
      // Build list of agent endpoint patterns to exclude from tracing
      // These are the endpoints the agent uses to send data to the API
      const agentEndpointPatterns: string[] = [];

      if (this.config.endpoint) {
        // Extract base URL using centralized method
        const baseUrl = this.getBaseUrl();

        // Exact ingest paths only. The bare "/log" / "/logs" / "/traces" fragments that
        // used to be here matched any customer route containing them, which erased those
        // routes from tracing entirely.
        agentEndpointPatterns.push(
          "/api/traces",
          "/api/log",
          "/api/logs",
          "/api/host-metrics",
          "/api/service/validate",
        );

        // Add full URLs with base URL
        agentEndpointPatterns.push(
          `${baseUrl}/api/traces`,
          `${baseUrl}/api/log`,
          `${baseUrl}/api/logs`,
          `${baseUrl}/api/host-metrics`,
          `${baseUrl}/api/service/validate`,
        );
      }

      this.traceMonitor = new TraceMonitor({
        serviceName: this.config.serviceName,
        serviceVersion: AGENT_VERSION,
        excludeAgentSpans: true, // Filter out agent's own HTTP calls
        agentEndpoints:
          agentEndpointPatterns.length > 0 ? agentEndpointPatterns : undefined,
        sharedSpanIdMap: this.spanIdMap, // Share spanIdMap to maintain parent-child relationships
        onSpan: (span) => {
          this.queueTraceSpan(span);
        },
      });

      // Start initialization synchronously - this will start the SDK immediately
      // The SDK.start() call is synchronous, so this completes before mongoose is required
      // We call initialize() which starts the SDK synchronously, then handle errors async
      this.traceMonitor.initialize().catch((err) => {
        diagnostics.error(
          `Failed to initialize trace monitoring: ${err}`,
        );
      });

      diagnostics.debug(
        "Trace monitoring initialization started (SDK will be ready before mongoose loads)",
      );

      // Custom DB instrumentations (Mongo + Redis)
      const instrumentationManager = InstrumentationManager.getInstance();

      if (this.config.enableMongoSpanCollection) {
        // Check if MongoInstrumentation was already registered (e.g., via -r polartrace)
        const mongoCallback = (mongoSpan: MongoSdkSpan) => {
          const traceSpan = this.convertMongoSpanToTraceSpan(mongoSpan);
          this.queueTraceSpan(traceSpan);
        };

        // Try to update existing MongoInstrumentation callback first
        const updated =
          instrumentationManager.updateMongoCallback(mongoCallback);

        // If no existing MongoInstrumentation, register a new one
        if (!updated) {
          instrumentationManager.register(
            new MongoInstrumentation(
              mongoCallback,
              (this.config as any).mongoose,
            ),
          );
          instrumentationManager.enableAll();
        }
      }

      if (this.config.enableRedisSpanCollection) {
        const redisCallback = (redisSpan: RedisSdkSpan) => {
          const traceSpan = this.convertRedisSpanToTraceSpan(redisSpan);
          this.queueTraceSpan(traceSpan);
        };

        const updated =
          instrumentationManager.updateRedisCallback(redisCallback);

        if (!updated) {
          instrumentationManager.register(new RedisInstrumentation(redisCallback));
          instrumentationManager.enableAll();
        }

        (globalThis as any).__POLARTRACE_REDIS_INSTRUMENTATION_ENABLED__ = true;
      }

      if (this.config.enablePostgresSpanCollection) {
        const postgresCallback = (pgSpan: PostgresSdkSpan) => {
          const traceSpan = this.convertPostgresSpanToTraceSpan(pgSpan);
          this.queueTraceSpan(traceSpan);
        };

        const updated =
          instrumentationManager.updatePostgresCallback(postgresCallback);

        if (!updated) {
          instrumentationManager.register(
            new PostgresInstrumentation(postgresCallback),
          );
          instrumentationManager.enableAll();
        }

        (globalThis as any).__POLARTRACE_POSTGRES_INSTRUMENTATION_ENABLED__ = true;
      }
    } catch (error: any) {
      diagnostics.error(
        `Failed to initialize trace monitoring: ${error.message}`,
      );
      // Don't throw - trace monitoring is optional
    }
  }

  /**
   * Queue a trace span with trace grouping logic.
   * Spans are cached by traceId until the parent span (HTTP request) completes.
   * This ensures all child spans (MongoDB, etc.) are sent together with their parent.
   */
  private queueTraceSpan(span: TraceSpan): void {
    const traceSpanData: TraceSpanData = {
      ...span,
    };

    const traceId = span.traceId;
    const isParentSpan = this.isParentSpan(span);

    // If this is a parent span completing, flush all cached spans for this trace
    if (isParentSpan) {
      const existingParentSpanId = this.parentSpanTracker.get(traceId);

      if (!existingParentSpanId) {
        // First time seeing this parent span - track it and cache it
        this.parentSpanTracker.set(traceId, span.spanId);
        if (!this.traceTimestamps.has(traceId)) {
          this.traceTimestamps.set(traceId, Date.now());
        }

        // Add parent span to cache
        if (!this.traceCache.has(traceId)) {
          this.traceCache.set(traceId, []);
        }
        this.traceCache.get(traceId)!.push(traceSpanData);

        // Parent span is complete when we receive it, so flush immediately
        this.flushTrace(traceId);
        return;
      }
    }

    // For child spans, add to cache (they'll be flushed when parent completes)
    if (!this.traceCache.has(traceId)) {
      this.traceCache.set(traceId, []);
      this.traceTimestamps.set(traceId, Date.now());
    }
    this.traceCache.get(traceId)!.push(traceSpanData);

    // Cleanup old traces periodically (every 100 spans to avoid performance impact)
    if (this.traceCache.size % 100 === 0) {
      this.cleanupOldTraces();
    }
  }

  /**
   * Check if a span is a parent/root span (typically an HTTP SERVER span)
   */
  private isParentSpan(span: TraceSpan): boolean {
    // A span is a parent if:
    // 1. It's a SERVER span (HTTP request)
    // 2. It has no parentSpanId (root span)
    // 3. It has HTTP attributes
    return (
      span.kind === "SERVER" &&
      !span.parentSpanId &&
      (span.attributes?.["http.method"] !== undefined ||
        span.attributes?.["http.request.method"] !== undefined)
    );
  }

  /**
   * Flush all spans for a given traceId to the queue (when parent span completes)
   */
  private flushTrace(traceId: string): void {
    const spans = this.traceCache.get(traceId);
    if (!spans || spans.length === 0) {
      // Cleanup tracking even if no spans (orphaned parent span)
      this.parentSpanTracker.delete(traceId);
      this.traceTimestamps.delete(traceId);
      return;
    }

    // Queue all spans in this trace
    for (const span of spans) {
      this.traceQueue.push({ span });
    }

    // Bound the queue so a long collector outage doesn't grow memory unbounded.
    if (this.traceQueue.length > this.TRACE_QUEUE_LIMIT) {
      const overflow = this.traceQueue.length - this.TRACE_QUEUE_LIMIT;
      this.traceQueue.splice(0, overflow);
    }

    diagnostics.debug(
      `Flushed trace ${traceId.substring(0, 8)}... with ${spans.length} spans (parent completed)`,
    );

    // Cleanup
    this.traceCache.delete(traceId);
    this.parentSpanTracker.delete(traceId);
    this.traceTimestamps.delete(traceId);
  }

  /**
   * Cleanup traces that have been cached for too long without completion
   * This prevents memory leaks from orphaned traces
   */
  private cleanupOldTraces(): void {
    const now = Date.now();
    const tracesToCleanup: string[] = [];

    for (const [traceId, timestamp] of this.traceTimestamps.entries()) {
      if (now - timestamp > this.TRACE_CACHE_TIMEOUT) {
        tracesToCleanup.push(traceId);
      }
    }

    for (const traceId of tracesToCleanup) {
      diagnostics.debug(
        `Cleaning up orphaned trace ${traceId.substring(0, 8)}... (timeout)`,
      );

      // Flush orphaned traces anyway (better than losing them)
      this.flushTrace(traceId);
    }
  }

  /**
   * Convert a Mongo SDK span into the generic TraceSpan format used by the agent.
   * If the Mongo span was created within an active OpenTelemetry span (e.g., during an HTTP request),
   * we use that span's traceId and set it as the parentSpanId to maintain the parent-child relationship.
   * This ensures MongoDB operations are properly nested under their parent HTTP request spans.
   */
  private convertMongoSpanToTraceSpan(mongoSpan: MongoSdkSpan): TraceSpan {
    const startMs = mongoSpan.startTime || Date.now();
    const endMs = mongoSpan.endTime ?? Date.now();
    const durationMs = mongoSpan.durationMs ?? endMs - startMs;

    const startTimeNs = startMs * 1_000_000;
    const endTimeNs = endMs * 1_000_000;

    // Use the traceId from the active span if available (maintains trace continuity with HTTP spans)
    // If no active span (standalone MongoDB operation), generate a new traceId
    const traceId = mongoSpan.traceId || this.generateUUID();

    // Convert parentSpanId from 16-char to 32-char UUID if it exists
    // This maintains parent-child relationships with HTTP spans that were converted in trace-monitor
    const parentSpanIdUUID = mongoSpan.parentSpanId
      ? this.convertSpanIdToUUID(mongoSpan.parentSpanId)
      : undefined;

    return {
      traceId,
      spanId: this.generateUUID(), // Generate 32-character UUID instead of 16-character ID
      parentSpanId: parentSpanIdUUID, // Converted to 32-character UUID to maintain parent-child relationship
      name: mongoSpan.name,
      kind: "CLIENT",
      startTime: startTimeNs,
      endTime: endTimeNs,
      duration: durationMs,
      status: {
        code: mongoSpan.error ? "ERROR" : "OK",
        message: mongoSpan.error?.message,
      },
      attributes: {
        "db.system": "mongodb",
        "db.name": mongoSpan.attributes.db,
        "db.operation": mongoSpan.attributes.command,
        "db.mongodb.collection": mongoSpan.attributes.collection,
        ...mongoSpan.attributes,
      },
    };
  }

  /**
   * Convert a Redis SDK span into the generic TraceSpan format.
   * Same trace/parent stitching as Mongo so Redis ops nest under their parent
   * HTTP span when one is active, and stand alone when there isn't one.
   */
  private convertRedisSpanToTraceSpan(redisSpan: RedisSdkSpan): TraceSpan {
    const startMs = redisSpan.startTime || Date.now();
    const endMs = redisSpan.endTime ?? Date.now();
    const durationMs = redisSpan.durationMs ?? endMs - startMs;

    const startTimeNs = startMs * 1_000_000;
    const endTimeNs = endMs * 1_000_000;

    const traceId = redisSpan.traceId || this.generateUUID();

    const parentSpanIdUUID = redisSpan.parentSpanId
      ? this.convertSpanIdToUUID(redisSpan.parentSpanId)
      : undefined;

    return {
      traceId,
      spanId: this.generateUUID(),
      parentSpanId: parentSpanIdUUID,
      name: redisSpan.name,
      kind: "CLIENT",
      startTime: startTimeNs,
      endTime: endTimeNs,
      duration: durationMs,
      status: {
        code: redisSpan.error ? "ERROR" : "OK",
        message: redisSpan.error?.message,
      },
      attributes: {
        "db.system": "redis",
        "db.operation": redisSpan.attributes.command,
        "db.redis.library": redisSpan.attributes.library,
        "db.statement.key": redisSpan.attributes.key,
        ...redisSpan.attributes,
      },
    };
  }

  /**
   * Convert a Postgres SDK span into the generic TraceSpan format.
   * Attribute keys follow OTel semconv (db.system / db.operation /
   * db.sql.table / db.statement) so the console's Database views pick the
   * spans up without any server-side changes.
   */
  private convertPostgresSpanToTraceSpan(pgSpan: PostgresSdkSpan): TraceSpan {
    const startMs = pgSpan.startTime || Date.now();
    const endMs = pgSpan.endTime ?? Date.now();
    const durationMs = pgSpan.durationMs ?? endMs - startMs;

    const traceId = pgSpan.traceId || this.generateUUID();

    const parentSpanIdUUID = pgSpan.parentSpanId
      ? this.convertSpanIdToUUID(pgSpan.parentSpanId)
      : undefined;

    return {
      traceId,
      spanId: this.generateUUID(),
      parentSpanId: parentSpanIdUUID,
      name: pgSpan.name,
      kind: "CLIENT",
      startTime: startMs * 1_000_000,
      endTime: endMs * 1_000_000,
      duration: durationMs,
      status: {
        code: pgSpan.error ? "ERROR" : "OK",
        message: pgSpan.error?.message,
      },
      attributes: {
        "db.system": "postgresql",
        "db.operation": pgSpan.attributes.operation,
        "db.sql.table": pgSpan.attributes.table,
        "db.statement": pgSpan.attributes.statement,
        ...pgSpan.attributes,
      },
    };
  }

  /**
   * Convert a 16-character span ID to a 32-character UUID
   * Maintains a mapping cache to ensure consistent parent-child relationships
   */
  private convertSpanIdToUUID(spanId: string): string {
    if (!spanId) return spanId;

    // If already 32 characters, assume it's already a UUID
    if (spanId.length === 32) {
      return spanId;
    }

    // Check if we've already converted this span ID
    if (this.spanIdMap.has(spanId)) {
      return this.spanIdMap.get(spanId)!;
    }

    // Generate a new UUID and store the mapping
    const uuidString = uuid().replace(/-/g, "");
    this.spanIdMap.set(spanId, uuidString);

    // Cleanup old mappings periodically to prevent memory leaks
    if (this.spanIdMap.size > 10000) {
      // Keep only the most recent 5000 entries
      const entries = Array.from(this.spanIdMap.entries());
      this.spanIdMap.clear();
      entries.slice(-5000).forEach(([key, value]) => {
        this.spanIdMap.set(key, value);
      });
    }

    return uuidString;
  }

  /**
   * Generate a UUID (32-character hex string)
   */
  private generateUUID(): string {
    return uuid().replace(/-/g, "");
  }

  /**
   * Return the current OpenTelemetry HTTP-server span's traceId (32-char hex)
   * if one is active, otherwise undefined. We stamp this onto the request log
   * so the UI can join request_logs → traces by trace_id and surface the
   * captured console output inside the trace detail view.
   */
  private getActiveTraceId(): string | undefined {
    try {
      const span = otelTrace.getActiveSpan();
      const id = span?.spanContext().traceId;
      // OTel's "invalid" sentinel is 32 zeros - treat as no trace.
      if (!id || /^0+$/.test(id)) return undefined;
      return id;
    } catch {
      return undefined;
    }
  }

  // Internal method - used by auto-attach mechanism, not for manual use
  middleware() {
    return (req: Request, res: Response, next: NextFunction) => {
      const startTimeMs = Date.now();
      const startTime = new Date(startTimeMs).toISOString();
      const capturedLogs: Array<{
        timestamp: string;
        level: string;
        args: any[];
      }> = [];
      // Request ID for tracking (can be used for future enhancements)
      this.generateRequestId();

      // Capture the OTel HTTP-server span's traceId on the way in - by the
      // time the response is sent the context may have already closed, so we
      // grab the id at the start of the request and stamp it on the log.
      // Converted to the same 32-char hex form the collector uses for traces,
      // so the UI can join request_logs → traces by trace_id directly.
      const traceId = this.getActiveTraceId();

      // Capture request data
      const requestLog: Partial<RequestLog> = {
        timestamp: startTime,
        method: req.method,
        path: req.path,
        userAgent: req.get("user-agent"),
        ip: this.getClientIp(req),
        host: req.get("host"),
        startTime: startTime,
        ...(traceId ? { traceId } : {}),
      };

      // Expose the log to the trailing error middleware (attached at listen
      // time, see autoAttachExpress): thrown/next(err) route errors travel
      // Express's ERROR middleware chain and never emit req/res "error"
      // events, so without this hand-off they'd log a bare 500 with no
      // message/stack.
      (req as any).__POLARTRACE_REQUEST_LOG__ = requestLog;

      // Optionally capture additional data
      if (this.config.captureHeaders) {
        requestLog.headers = this.redactSensitive(req.headers) as Record<
          string,
          string | string[] | undefined
        >;
      }

      if (this.config.captureQuery && Object.keys(req.query).length > 0) {
        requestLog.query = req.query;
      }

      // Intercept console methods if enabled
      let consoleIntercepted = false;
      if (this.config.captureConsoleLogs) {
        this.interceptConsole(capturedLogs);
        consoleIntercepted = true;
      }

      // Capture errors
      const errorHandler = (err: Error) => {
        requestLog.error = {
          message: err.message,
          stack: err.stack,
          name: err.name,
        };
      };

      // Intercept response to capture status code and duration
      const originalSend = res.send;
      const originalJson = res.json;
      let isLogged = false; // Flag to prevent double logging

      res.send = (data: any): Response => {
        if (!isLogged) {
          isLogged = true;
          return this.handleResponse(
            req,
            res,
            originalSend,
            data,
            startTimeMs,
            requestLog,
            capturedLogs,
            consoleIntercepted,
          );
        }
        return originalSend.call(res, data);
      };

      res.json = (data: any): Response => {
        if (!isLogged) {
          isLogged = true;
          return this.handleResponse(
            req,
            res,
            originalJson,
            data,
            startTimeMs,
            requestLog,
            capturedLogs,
            consoleIntercepted,
          );
        }
        return originalJson.call(res, data);
      };

      // Handle unhandled errors
      req.on("error", errorHandler);
      res.on("error", errorHandler);

      // Fallback: responses written via res.end() directly - Express's
      // finalhandler after a thrown route error, bare 404s without a
      // catch-all, res.sendFile/streams - bypass the send/json wrappers
      // above, and their request logs would otherwise be lost entirely.
      res.on("finish", () => {
        if (!isLogged) {
          isLogged = true;
          this.handleResponse(
            req,
            res,
            () => res,
            undefined,
            startTimeMs,
            requestLog,
            capturedLogs,
            consoleIntercepted,
          );
        }
      });

      next();
    };
  }

  /**
   * Koa middleware producing the same request-log payload as the Express
   * middleware. Prepended automatically by the koa require hook (see
   * autoAttachKoaHook), or usable manually: `app.use(agent.koaMiddleware())`.
   * Thrown route errors are captured into `log.error` and re-thrown so Koa's
   * own error handling is unaffected.
   */
  koaMiddleware() {
    return async (ctx: any, next: () => Promise<any>) => {
      const startTimeMs = Date.now();
      const startTime = new Date(startTimeMs).toISOString();
      const capturedLogs: Array<{
        timestamp: string;
        level: string;
        args: any[];
      }> = [];
      const traceId = this.getActiveTraceId();

      const requestLog: Partial<RequestLog> = {
        timestamp: startTime,
        method: ctx.method,
        path: ctx.path,
        userAgent: ctx.get("user-agent"),
        ip: ctx.ip,
        host: ctx.host,
        startTime,
        ...(traceId ? { traceId } : {}),
      };
      if (this.config.captureHeaders) {
        requestLog.headers = this.redactSensitive(ctx.headers);
      }
      if (
        this.config.captureQuery &&
        ctx.query &&
        Object.keys(ctx.query).length > 0
      ) {
        requestLog.query = ctx.query;
      }

      let consoleIntercepted = false;
      if (this.config.captureConsoleLogs) {
        this.interceptConsole(capturedLogs);
        consoleIntercepted = true;
      }

      let thrown: any;
      try {
        await next();
      } catch (err: any) {
        thrown = err;
        requestLog.error = {
          message: String(err?.message ?? err),
          stack: err?.stack,
          name: err?.name,
        };
      } finally {
        const endTimeMs = Date.now();
        requestLog.statusCode = thrown
          ? thrown.status || thrown.statusCode || 500
          : ctx.status;
        requestLog.duration = endTimeMs - startTimeMs;
        requestLog.endTime = new Date(endTimeMs).toISOString();
        if (this.config.captureBody && ctx.request?.body) {
          requestLog.body = this.redactSensitive(ctx.request.body);
        }
        if (consoleIntercepted) {
          this.restoreConsole();
          if (capturedLogs.length > 0) {
            requestLog.consoleLogs = capturedLogs;
          }
        }
        this.logRequest(requestLog as RequestLog);
      }
      if (thrown) throw thrown;
    };
  }

  /**
   * Attach request-log capture to a Fastify instance via its hook API - the
   * Express-style middleware cannot run on Fastify. Applied automatically by
   * the fastify require hook (see autoAttachFastifyHook), or manually:
   * `agent.instrumentFastify(app)` before routes are registered.
   */
  instrumentFastify(app: any): void {
    if (!app || typeof app.addHook !== "function") return;
    if (app.__POLARTRACE_MIDDLEWARE_ATTACHED__) return;
    app.__POLARTRACE_MIDDLEWARE_ATTACHED__ = true;

    interface FastifyReqState {
      startTimeMs: number;
      traceId?: string;
      capturedLogs: Array<{ timestamp: string; level: string; args: any[] }>;
      consoleIntercepted: boolean;
      error?: any;
    }
    const states = new WeakMap<object, FastifyReqState>();

    app.addHook("onRequest", async (request: any) => {
      const state: FastifyReqState = {
        startTimeMs: Date.now(),
        traceId: this.getActiveTraceId(),
        capturedLogs: [],
        consoleIntercepted: false,
      };
      if (this.config.captureConsoleLogs) {
        this.interceptConsole(state.capturedLogs);
        state.consoleIntercepted = true;
      }
      states.set(request, state);
      // Also ride the state on the request itself: NestJS (Fastify platform)
      // swallows route exceptions in its exception filters before Fastify's
      // onError can fire, and the Nest hook (autoAttachNestErrorHook) can only
      // reach this request object - not the WeakMap above.
      (request as any).__POLARTRACE_FASTIFY_STATE__ = state;
    });

    app.addHook("onError", async (request: any, _reply: any, error: any) => {
      const state = states.get(request);
      if (state) state.error = error;
    });

    app.addHook("onResponse", async (request: any, reply: any) => {
      const state = states.get(request);
      if (!state) return;
      states.delete(request);

      const endTimeMs = Date.now();
      const startTime = new Date(state.startTimeMs).toISOString();
      const requestLog: Partial<RequestLog> = {
        timestamp: startTime,
        method: request.method,
        path: String(request.raw?.url || request.url || "").split("?")[0],
        statusCode: reply.statusCode,
        duration: endTimeMs - state.startTimeMs,
        startTime,
        endTime: new Date(endTimeMs).toISOString(),
        userAgent: request.headers?.["user-agent"],
        ip: request.ip,
        host: request.headers?.host,
        ...(state.traceId ? { traceId: state.traceId } : {}),
      };
      if (this.config.captureHeaders) {
        requestLog.headers = this.redactSensitive(request.headers);
      }
      if (
        this.config.captureQuery &&
        request.query &&
        Object.keys(request.query).length > 0
      ) {
        requestLog.query = request.query;
      }
      if (this.config.captureBody && request.body) {
        requestLog.body = this.redactSensitive(request.body);
      }
      if (state.consoleIntercepted) {
        this.restoreConsole();
        if (state.capturedLogs.length > 0) {
          requestLog.consoleLogs = state.capturedLogs;
        }
      }
      if (state.error) {
        requestLog.error = {
          message: String(state.error?.message ?? state.error),
          stack: state.error?.stack,
          name: state.error?.name,
        };
      }
      this.logRequest(requestLog as RequestLog);
    });
  }

  private handleResponse(
    req: Request,
    res: Response,
    originalMethod: Function,
    data: any,
    startTimeMs: number,
    requestLog: Partial<RequestLog>,
    capturedLogs: Array<{ timestamp: string; level: string; args: any[] }>,
    consoleIntercepted: boolean,
  ): Response {
    const endTimeMs = Date.now();
    const duration = endTimeMs - startTimeMs;
    const endTime = new Date(endTimeMs).toISOString();
    requestLog.statusCode = res.statusCode;
    requestLog.duration = duration;
    requestLog.endTime = endTime;

    // Read the body here rather than on the way in: the agent's middleware is
    // prepended, so at request entry express.json() has not run and req.body
    // is still undefined.
    if (this.config.captureBody && req?.body) {
      requestLog.body = this.redactSensitive(req.body);
    }

    // Restore original console methods
    if (consoleIntercepted) {
      this.restoreConsole();
      if (capturedLogs.length > 0) {
        requestLog.consoleLogs = capturedLogs;
      }
    }

    // Log the request
    this.logRequest(requestLog as RequestLog);

    return originalMethod.call(res, data);
  }

  private interceptConsole(
    capturedLogs: Array<{ timestamp: string; level: string; args: any[] }>,
  ): void {
    if (!this.originalConsoleLog) {
      this.originalConsoleLog = console.log;
      this.originalConsoleError = console.error;
      this.originalConsoleWarn = console.warn;
      this.originalConsoleInfo = console.info;
    }

    console.log = (...args: any[]) => {
      capturedLogs.push({
        timestamp: new Date().toISOString(),
        level: "log",
        args: args,
      });
      this.originalConsoleLog!.apply(console, args);
    };

    console.error = (...args: any[]) => {
      capturedLogs.push({
        timestamp: new Date().toISOString(),
        level: "error",
        args: args,
      });
      this.originalConsoleError!.apply(console, args);
    };

    console.warn = (...args: any[]) => {
      capturedLogs.push({
        timestamp: new Date().toISOString(),
        level: "warn",
        args: args,
      });
      this.originalConsoleWarn!.apply(console, args);
    };

    console.info = (...args: any[]) => {
      capturedLogs.push({
        timestamp: new Date().toISOString(),
        level: "info",
        args: args,
      });
      this.originalConsoleInfo!.apply(console, args);
    };
  }

  private restoreConsole(): void {
    if (this.originalConsoleLog) {
      console.log = this.originalConsoleLog;
      console.error = this.originalConsoleError!;
      console.warn = this.originalConsoleWarn!;
      console.info = this.originalConsoleInfo!;
    }
  }

  private redactSensitive(body: any): any {
    if (typeof body !== "object" || body === null) {
      return body;
    }

    const sensitiveFields = SENSITIVE_FIELDS;
    const sanitized = Array.isArray(body) ? [...body] : { ...body };

    for (const key in sanitized) {
      if (
        sensitiveFields.some((field) =>
          key.toLowerCase().includes(field.toLowerCase()),
        )
      ) {
        sanitized[key] = "[REDACTED]";
      } else if (
        typeof sanitized[key] === "object" &&
        sanitized[key] !== null
      ) {
        sanitized[key] = this.redactSensitive(sanitized[key]);
      }
    }

    return sanitized;
  }

  private getClientIp(req: Request): string {
    return (
      (req.headers["x-forwarded-for"] as string)?.split(",")[0] ||
      (req.headers["x-real-ip"] as string) ||
      req.socket.remoteAddress ||
      req.ip ||
      "unknown"
    );
  }

  private generateRequestId(): string {
    return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
  }

  private logRequest(log: RequestLog): void {
    // Always send to API (endpoint is always configured by default)
    this.queueLog(log);
  }

  private queueLog(log: RequestLog): void {
    this.logQueue.push({ log });

    // Bound the queue so a long collector outage doesn't grow memory unbounded.
    if (this.logQueue.length > this.LOG_QUEUE_LIMIT) {
      const overflow = this.logQueue.length - this.LOG_QUEUE_LIMIT;
      this.logQueue.splice(0, overflow);
    }

    diagnostics.debug(
      `Log queued. Queue size: ${this.logQueue.length}`,
    );
  }

  private async flushLogs(): Promise<void> {
    // Only flush if there are logs in the queue
    if (this.isFlushing || this.logQueue.length === 0) {
      return;
    }

    // Back off after retained failures - skip ticks until the deadline passes.
    if (Date.now() < this.logBackoffUntil) {
      diagnostics.debug(
        `Log flush skipped - backing off until ${new Date(this.logBackoffUntil).toISOString()}`,
      );
      return;
    }

    this.isFlushing = true;
    const batchLength = Math.min(this.logQueue.length, this.MAX_BATCH_SIZE);
    // Create a copy of logs to send, but DON'T remove from queue yet
    const logsToSend = this.logQueue
      .slice(0, batchLength)
      .map((item) => item.log);

    diagnostics.debug(
      `Attempting to send ${logsToSend.length} logs...`,
    );

    try {
      // Send logs to API
      await this.sendLogsToAPI(logsToSend);

      // Only remove from queue AFTER successful send
      this.logQueue.splice(0, batchLength);
      this.logConsecutiveFailures = 0;
      this.logBackoffUntil = 0;
      diagnostics.debug(
        `Successfully sent ${logsToSend.length} logs and removed from queue`,
      );
    } catch (error: any) {
      const status = error?.response?.status;
      if (this.shouldRetainBatch(status)) {
        // Transient failure (429/5xx/network) - keep logs in queue and back off.
        this.logConsecutiveFailures++;
        this.logBackoffUntil =
          Date.now() +
          this.nextBackoffMs(
            this.logConsecutiveFailures,
            error?.response?.retryAfterSeconds,
          );
        diagnostics.debug(
          `Failed to send logs to API: ${error?.message}`,
        );
        diagnostics.debug(
          `Keeping ${batchLength} logs in queue for retry (failure #${this.logConsecutiveFailures})`,
        );
      } else {
        // Deterministic 4xx rejection (auth/validation) - the collector will
        // never accept this batch, so drop it rather than retrying forever.
        this.logQueue.splice(0, batchLength);
        this.logConsecutiveFailures = 0;
        this.logBackoffUntil = 0;
        diagnostics.warnOnce(
          `drop:logs:${status}`,
          `dropped ${batchLength} logs rejected by collector (status ${status})`,
        );
      }
    } finally {
      this.isFlushing = false;
    }
  }

  private async sendLogsToAPI(logs: RequestLog[]): Promise<void> {
    if (!this.config.endpoint) {
      return;
    }

    try {
      const response = await this.postJson(this.config.endpoint, { logs });
      if (response.status >= 400) {
        const err: any = new Error(`API returned status ${response.status}`);
        err.response = response;
        throw err;
      }
    } catch (error: any) {
      if (error?.response) {
        diagnostics.debug(
          `collector responded with error: ${error.response.status} ${JSON.stringify(error.response.data, null, 2)}`,
        );
      } else if (error?.name === "AbortError") {
        diagnostics.debug("no response received from collector");
      } else {
        diagnostics.debug(`failed to send logs: ${error?.message}`);
      }
      throw error;
    }
  }

  private async flushTraces(): Promise<void> {
    // Cleanup old traces before flushing
    this.cleanupOldTraces();

    diagnostics.debug(
      `flushTraces called - queue size: ${this.traceQueue.length}, isFlushing: ${this.isTraceFlushing}`,
    );

    if (this.isTraceFlushing || this.traceQueue.length === 0) {
      if (this.isTraceFlushing) {
        diagnostics.debug(
          "Trace flush skipped - already flushing",
        );
      } else {
        diagnostics.debug(
          "Trace flush skipped - queue is empty",
        );
      }
      return;
    }

    // Back off after retained failures - skip ticks until the deadline passes.
    if (Date.now() < this.traceBackoffUntil) {
      diagnostics.debug(
        `Trace flush skipped - backing off until ${new Date(this.traceBackoffUntil).toISOString()}`,
      );
      return;
    }

    this.isTraceFlushing = true;
    const batchLength = Math.min(this.traceQueue.length, this.MAX_BATCH_SIZE);

    // Create a copy of spans to send (don't modify queue yet)
    const spansToSend = this.traceQueue
      .slice(0, batchLength)
      .map((item) => item.span);

    diagnostics.debug(
      `Attempting to send ${spansToSend.length} trace spans...`,
    );
    diagnostics.debug(
      `Current queue size before send: ${this.traceQueue.length}`,
    );

    try {
      const response = await this.sendTracesToAPI(spansToSend);

      // Only remove spans from queue if API call was successful (2xx status)
      const status = response?.status;
      if (response && status >= 200 && status < 300) {
        // Successfully sent - remove spans from queue
        // Use splice to remove exactly batchLength items from the beginning
        const removed = this.traceQueue.splice(0, batchLength);
        const removedCount = removed.length;
        this.traceConsecutiveFailures = 0;
        this.traceBackoffUntil = 0;

        diagnostics.debug(
          `Successfully sent ${spansToSend.length} trace spans (status: ${status})`,
        );
        diagnostics.debug(
          `Removed ${removedCount} spans from queue`,
        );
        diagnostics.debug(
          `Remaining queue size: ${this.traceQueue.length}`,
        );

        // Verify removal
        if (removedCount !== batchLength) {
          diagnostics.debug(
            `Expected to remove ${batchLength} spans but removed ${removedCount}`,
          );
        }
      } else if (this.shouldRetainBatch(status)) {
        // 429 backpressure - keep spans in queue and back off before retrying.
        this.traceConsecutiveFailures++;
        this.traceBackoffUntil =
          Date.now() +
          this.nextBackoffMs(
            this.traceConsecutiveFailures,
            response?.retryAfterSeconds,
          );
        diagnostics.debug(
          `API returned status ${status}, keeping ${batchLength} spans in queue for retry (failure #${this.traceConsecutiveFailures})`,
        );
      } else {
        // Deterministic 4xx rejection (auth/validation) - the collector will
        // never accept this batch, so drop it rather than retrying forever.
        this.traceQueue.splice(0, batchLength);
        this.traceConsecutiveFailures = 0;
        this.traceBackoffUntil = 0;
        diagnostics.warnOnce(
          `drop:traces:${status}`,
          `dropped ${batchLength} trace spans rejected by collector (status ${status})`,
        );
      }
    } catch (error: any) {
      // 5xx or network error - don't remove spans from queue, retry with backoff.
      this.traceConsecutiveFailures++;
      this.traceBackoffUntil =
        Date.now() + this.nextBackoffMs(this.traceConsecutiveFailures);
      diagnostics.debug(
        `Failed to send trace spans to API: ${error?.message}`,
      );
      diagnostics.debug(
        `Keeping ${batchLength} trace spans in queue for retry (failure #${this.traceConsecutiveFailures})`,
      );
      diagnostics.debug(
        `Current queue size: ${this.traceQueue.length}`,
      );
    } finally {
      this.isTraceFlushing = false;
    }
  }

  private async sendTracesToAPI(spans: TraceSpanData[]): Promise<any> {
    if (!this.config.endpoint) {
      throw new Error("Endpoint not configured");
    }

    // Construct traces endpoint
    let tracesEndpoint: string;
    const endpoint = this.config.endpoint.trim();

    if (endpoint.endsWith("/log")) {
      tracesEndpoint = endpoint.replace("/log", "/traces");
    } else if (endpoint.includes("/api/log")) {
      tracesEndpoint = endpoint.replace("/api/log", "/api/traces");
    } else {
      // Use centralized baseUrl method
      const baseUrl = this.getBaseUrl();
      tracesEndpoint = `${baseUrl}/traces`;
    }

    tracesEndpoint = tracesEndpoint.replace(/([^:]\/)\/+/g, "$1");

    diagnostics.debug(
      `Sending traces to endpoint: ${tracesEndpoint}`,
    );

    try {
      const response = await this.postJson(tracesEndpoint, { spans });

      diagnostics.debug(
        `API response received - status: ${response.status}`,
      );

      return response;
    } catch (error: any) {
      diagnostics.debug(
        `Failed to send trace spans: ${error?.message}`,
      );
      if (error?.response) {
        diagnostics.debug(
          `Response status: ${error.response.status}`,
        );
        diagnostics.debug(
          `Response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Flush queued host-metric samples to the collector. Transient failures
   * (429/5xx/network) keep the batch for retry with backoff; deterministic
   * 4xx rejections drop it (same semantics as logs and traces).
   */
  private async flushHostMetrics(): Promise<void> {
    if (this.isHostMetricsFlushing || this.hostMetricsQueue.length === 0) {
      return;
    }

    // Back off after retained failures - skip ticks until the deadline passes.
    if (Date.now() < this.hostMetricsBackoffUntil) {
      diagnostics.debug(
        `Host metrics flush skipped - backing off until ${new Date(this.hostMetricsBackoffUntil).toISOString()}`,
      );
      return;
    }

    this.isHostMetricsFlushing = true;
    const batchLength = Math.min(
      this.hostMetricsQueue.length,
      this.MAX_BATCH_SIZE,
    );
    const samplesToSend = this.hostMetricsQueue
      .slice(0, batchLength)
      .map((item) => item.sample);

    try {
      const response = await this.sendHostMetricsToAPI(samplesToSend);
      const status = response?.status;

      if (response && status >= 200 && status < 300) {
        this.hostMetricsQueue.splice(0, batchLength);
        this.hostMetricsConsecutiveFailures = 0;
        this.hostMetricsBackoffUntil = 0;
        diagnostics.debug(
          `Successfully sent ${samplesToSend.length} host metric samples`,
        );
      } else if (this.shouldRetainBatch(status)) {
        // 429 backpressure - keep samples in queue and back off before retrying.
        this.hostMetricsConsecutiveFailures++;
        this.hostMetricsBackoffUntil =
          Date.now() +
          this.nextBackoffMs(
            this.hostMetricsConsecutiveFailures,
            response?.retryAfterSeconds,
          );
        diagnostics.debug(
          `API returned status ${status}, keeping ${batchLength} host metric samples in queue for retry (failure #${this.hostMetricsConsecutiveFailures})`,
        );
      } else {
        // Deterministic 4xx rejection (auth/validation) - the collector will
        // never accept this batch, so drop it rather than retrying forever.
        this.hostMetricsQueue.splice(0, batchLength);
        this.hostMetricsConsecutiveFailures = 0;
        this.hostMetricsBackoffUntil = 0;
        diagnostics.warnOnce(
          `drop:host-metrics:${status}`,
          `dropped ${batchLength} host metric samples rejected by collector (status ${status})`,
        );
      }
    } catch (error: any) {
      // 5xx or network error - keep samples in queue, retry with backoff.
      this.hostMetricsConsecutiveFailures++;
      this.hostMetricsBackoffUntil =
        Date.now() + this.nextBackoffMs(this.hostMetricsConsecutiveFailures);
      diagnostics.debug(
        `Failed to send host metrics: ${error?.message}`,
      );
      diagnostics.debug(
        `Keeping ${batchLength} host metric samples in queue for retry (failure #${this.hostMetricsConsecutiveFailures})`,
      );
    } finally {
      this.isHostMetricsFlushing = false;
    }
  }

  /**
   * POST host-metric samples to the collector. The collector's route is
   * derived from the same base URL the agent uses for logs/traces.
   */
  private async sendHostMetricsToAPI(
    samples: HostMetricSample[],
  ): Promise<{ status: number; data: any; retryAfterSeconds?: number }> {
    if (!this.config.endpoint) {
      throw new Error("Endpoint not configured");
    }

    const baseUrl = this.getBaseUrl();
    const hostMetricsEndpoint = `${baseUrl}/api/host-metrics`.replace(
      /([^:]\/)\/+/g,
      "$1",
    );

    return this.postJson(hostMetricsEndpoint, { samples });
  }

  /**
   * Flush everything still queued and stop the timers, waiting for delivery.
   *
   * `destroy()` starts the same flushes but does not wait, which is fine for an explicit
   * teardown inside a running process and useless on exit: the process was gone before the
   * requests completed, so the last batch of telemetry - typically the requests served
   * immediately before a deploy or a scale-down - was simply lost.
   *
   * Bounded on purpose: a collector that is slow or unreachable must never hold a shutting
   * down application open.
   */
  public async shutdown(timeoutMs: number = 3000): Promise<void> {
    if (this.flushTimer) clearInterval(this.flushTimer);
    if (this.traceFlushTimer) clearInterval(this.traceFlushTimer);
    if (this.hostMetricsFlushTimer) clearInterval(this.hostMetricsFlushTimer);

    // Spans whose parent request never completed still describe real work - send them.
    for (const traceId of this.traceCache.keys()) {
      this.flushTrace(traceId);
    }

    const pending: Promise<unknown>[] = [];
    if (this.logQueue.length > 0) pending.push(this.flushLogs());
    if (this.traceQueue.length > 0) pending.push(this.flushTraces());
    if (this.hostMetricsQueue.length > 0) pending.push(this.flushHostMetrics());

    if (pending.length > 0) {
      const settled = Promise.allSettled(pending);
      const deadline = new Promise((resolve) => {
        const timer = setTimeout(resolve, timeoutMs);
        if (typeof timer.unref === "function") timer.unref();
      });
      await Promise.race([settled, deadline]);
    }

    if (this.traceMonitor) this.traceMonitor.destroy();
    if (this.hostMetricsMonitor) this.hostMetricsMonitor.destroy();
    diagnostics.close();
  }

  public destroy(): void {
    // Stop timers
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    if (this.traceFlushTimer) {
      clearInterval(this.traceFlushTimer);
    }
    if (this.hostMetricsFlushTimer) {
      clearInterval(this.hostMetricsFlushTimer);
    }

    // Flush all cached traces (even if parent hasn't completed) before shutdown
    for (const traceId of this.traceCache.keys()) {
      this.flushTrace(traceId);
    }

    // Flush remaining logs and traces
    if (this.logQueue.length > 0) {
      this.flushLogs().catch((err) => {
        const errorMsg = `Error flushing logs on destroy: ${err}`;
        diagnostics.debug(errorMsg);
      });
    }

    if (this.traceQueue.length > 0) {
      this.flushTraces().catch((err) => {
        const errorMsg = `Error flushing traces on destroy: ${err}`;
        diagnostics.debug(errorMsg);
      });
    }

    if (this.hostMetricsQueue.length > 0) {
      this.flushHostMetrics().catch((err) => {
        const errorMsg = `Error flushing host metrics on destroy: ${err}`;
        diagnostics.debug(errorMsg);
      });
    }

    // Destroy monitors
    if (this.traceMonitor) {
      this.traceMonitor.destroy();
    }
    if (this.hostMetricsMonitor) {
      this.hostMetricsMonitor.destroy();
    }

    diagnostics.close();
  }
}

/**
 * Read a boolean environment variable. Accepts "1" and "true" (any case);
 * anything else - including unset - falls back to `fallback`.
 */
function envFlag(name: string, fallback: boolean): boolean {
  const raw = process.env[name]?.trim().toLowerCase();
  if (raw === undefined || raw === "") {
    return fallback;
  }
  return raw === "1" || raw === "true";
}

/**
 * Auto-initialize PolarTrace when the module is preloaded via `node -r polartrace`.
 * Configuration comes from the environment so applications need no code changes.
 *
 * Required:
 * - POLARTRACE_APP_NAME              service name reported to the collector
 * - POLARTRACE_LICENSE_KEY           API key
 *
 * Optional:
 * - POLARTRACE_ENDPOINT              override the collector URL
 * - POLARTRACE_LOG_FILE              append agent diagnostics to this path
 * - POLARTRACE_ENABLE_CONSOLE_LOG    mirror verbose diagnostics to stderr
 * - POLARTRACE_DISABLE_HOST_METRICS  turn off CPU/memory/event-loop sampling
 * - POLARTRACE_DISABLE_MONGO_SPANS   turn off MongoDB span collection
 * - POLARTRACE_DISABLE_REDIS_SPANS   turn off Redis span collection
 * - POLARTRACE_DISABLE_POSTGRES_SPANS turn off PostgreSQL span collection
 */
function autoInitFromEnv() {
  const serviceName = process.env.POLARTRACE_APP_NAME;
  const apiKey = process.env.POLARTRACE_LICENSE_KEY;

  // Only auto-init when both key pieces of config are present
  if (!serviceName || !apiKey) {
    return;
  }

  const globalObj = globalThis as any;

  // Ensure we don't create multiple agents if required more than once
  if (globalObj.__POLARTRACE_AGENT__) {
    return;
  }

  const endpoint = process.env.POLARTRACE_ENDPOINT?.trim() || undefined;
  const logFile = process.env.POLARTRACE_LOG_FILE?.trim() || undefined;

  let agent: PolarTrace;
  try {
    agent = new PolarTrace({
      apiKey,
      serviceName,
      enableConsoleLog: envFlag("POLARTRACE_ENABLE_CONSOLE_LOG", false),
      enableHostMetrics: !envFlag("POLARTRACE_DISABLE_HOST_METRICS", false),
      enableMongoSpanCollection: !envFlag(
        "POLARTRACE_DISABLE_MONGO_SPANS",
        false,
      ),
      enableRedisSpanCollection: !envFlag(
        "POLARTRACE_DISABLE_REDIS_SPANS",
        false,
      ),
      enablePostgresSpanCollection: !envFlag(
        "POLARTRACE_DISABLE_POSTGRES_SPANS",
        false,
      ),
      ...(endpoint ? { endpoint } : {}),
      ...(logFile ? { logFile } : {}),
    });
  } catch (error: any) {
    // Preload runs before the application's own code. A misconfigured agent
    // must disable itself and say why - never take the host process down.
    diagnostics.configure({
      verbose: envFlag("POLARTRACE_ENABLE_CONSOLE_LOG", false),
      filePath: logFile,
    });
    diagnostics.error(`agent disabled: ${error?.message ?? error}`);
    return;
  }

  globalObj.__POLARTRACE_AGENT__ = agent;

  installShutdownHooks(agent);

  // Automatically attach middleware to Express, Fastify and Koa apps
  autoAttachExpressMiddleware(agent);
  autoAttachFastifyHook(agent);
  autoAttachKoaHook(agent);
  autoAttachNestErrorHook();
}

/**
 * Deliver whatever is still queued when the process is going away.
 *
 * Without this the requests served in the last flush interval never left the process, so a
 * deploy, a scale-down or a short-lived job lost its final telemetry - and the gap looked
 * like an outage rather than a shutdown.
 *
 * The application stays in charge of its own lifecycle: if it handles the signal itself we
 * only flush, and we exit the process only when nothing else is listening.
 */
function installShutdownHooks(agent: PolarTrace): void {
  const globalObj = globalThis as any;
  if (globalObj.__POLARTRACE_SHUTDOWN_HOOKS__) return;
  globalObj.__POLARTRACE_SHUTDOWN_HOOKS__ = true;

  let finished: Promise<void> | null = null;
  const flushOnce = (): Promise<void> => {
    if (!finished) finished = agent.shutdown().catch(() => undefined);
    return finished;
  };

  process.on("beforeExit", () => {
    void flushOnce();
  });

  for (const signal of ["SIGTERM", "SIGINT"] as const) {
    process.on(signal, () => {
      const otherListeners = process.listenerCount(signal) - 1;
      void flushOnce().then(() => {
        // Only take the process down when the application has no handler of its own -
        // otherwise its shutdown sequence owns the exit and we must not pre-empt it.
        if (otherListeners <= 0) {
          process.exit(signal === "SIGINT" ? 130 : 143);
        }
      });
    });
  }
}

/**
 * Automatically attach PolarTrace middleware to Express apps when they're created.
 * This patches Express so that any app created will automatically have our middleware attached.
 */
function autoAttachExpressMiddleware(agent: PolarTrace): void {
  onModuleLoad("express", (module: any) => {
    if (typeof module !== "function") return;

    const originalExpress = module;
    const patchedExpress = function (this: any) {
      const app = originalExpress.apply(this, arguments);

      // Check if middleware is already attached (avoid double attachment)
      if (!(app as any).__POLARTRACE_MIDDLEWARE_ATTACHED__) {
        (app as any).__POLARTRACE_MIDDLEWARE_ATTACHED__ = true;

        // We are loaded via -r polartrace, so we run before user code and this
        // middleware lands early in the stack.
        try {
          app.use(agent.middleware());
        } catch {
          // Silent fail - middleware attachment is best effort
        }

        // Deferred error capture: a 4-arg middleware registered BEFORE the
        // routes would never see their errors, so it is appended at listen()
        // time when the user's stack is complete. It records the error onto
        // the request log and re-delegates with next(err), leaving default
        // (or user) error handling byte-identical.
        try {
          const originalListen = app.listen;
          app.listen = function (this: any, ...args: any[]) {
            if (!(app as any).__POLARTRACE_ERROR_MW_ATTACHED__) {
              (app as any).__POLARTRACE_ERROR_MW_ATTACHED__ = true;
              app.use((err: any, req: any, _res: any, next: any) => {
                const log = req?.__POLARTRACE_REQUEST_LOG__;
                if (err && log && !log.error) {
                  log.error = {
                    message: String(err?.message ?? err),
                    stack: err?.stack,
                    name: err?.name,
                  };
                }
                next(err);
              });
            }
            return originalListen.apply(this, args);
          };
        } catch {
          // Silent fail - deferred error capture is best effort
        }
      }

      return app;
    };

    // Copy Express properties (like Router, static, json, urlencoded, etc.)
    Object.setPrototypeOf(patchedExpress, originalExpress);
    Object.keys(originalExpress).forEach((key) => {
      (patchedExpress as any)[key] = (originalExpress as any)[key];
    });

    return patchedExpress;
  });
}

/**
 * Automatically attach request-log capture to Fastify apps. Wraps the
 * fastify() factory so every created instance gets the agent's hooks before
 * any routes are registered.
 */
function autoAttachFastifyHook(agent: PolarTrace): void {
  onModuleLoad("fastify", (module: any) => {
    if (typeof module !== "function") return;

    const originalFactory: any = module;
    if (!originalFactory.__POLARTRACE_PATCHED_FACTORY__) {
      const patchedFactory = function (this: any, ...args: any[]) {
        const app = originalFactory.apply(this, args);
        try {
          agent.instrumentFastify(app);
        } catch {
          // Silent fail - hook attachment is best effort
        }
        return app;
      };
      Object.setPrototypeOf(patchedFactory, originalFactory);
      Object.keys(originalFactory).forEach((key) => {
        (patchedFactory as any)[key] = originalFactory[key];
      });
      // fastify's self-referential exports must point at the wrapper too,
      // so `require("fastify").fastify` / ESM default land on the patch.
      (patchedFactory as any).fastify = patchedFactory;
      (patchedFactory as any).default = patchedFactory;
      originalFactory.__POLARTRACE_PATCHED_FACTORY__ = patchedFactory;
    }
    return originalFactory.__POLARTRACE_PATCHED_FACTORY__;
  });
}

/**
 * Automatically attach request-log capture to Koa apps. Patches
 * Koa.prototype.callback (invoked by both app.listen() and
 * http.createServer(app.callback())) to prepend the agent middleware once
 * per app, keeping the exported class identity intact for subclassing.
 */
function autoAttachKoaHook(agent: PolarTrace): void {
  onModuleLoad("koa", (module: any) => {
    if (
      typeof module !== "function" ||
      !module.prototype ||
      typeof module.prototype.callback !== "function" ||
      module.__POLARTRACE_PATCHED__
    ) {
      return;
    }

    module.__POLARTRACE_PATCHED__ = true;
    const originalCallback = module.prototype.callback;
    module.prototype.callback = function (this: any, ...args: any[]) {
      if (!this.__POLARTRACE_MIDDLEWARE_ATTACHED__) {
        this.__POLARTRACE_MIDDLEWARE_ATTACHED__ = true;
        try {
          this.middleware.unshift(agent.koaMiddleware());
        } catch {
          // Silent fail - middleware attachment is best effort
        }
      }
      return originalCallback.apply(this, args);
    };
  });
}

/**
 * Record a NestJS route exception onto the agent's per-request log. Works for
 * both Nest platforms: on Express the log rides on the request object
 * (`__POLARTRACE_REQUEST_LOG__`), on Fastify the per-request state does
 * (`__POLARTRACE_FASTIFY_STATE__`, see instrumentFastify's onRequest hook).
 *
 * 4xx HttpExceptions (NotFoundException on unmatched routes, validation
 * 400s...) are EXPECTED control flow and deliberately not recorded as errors;
 * anything reporting >= 500 - or carrying no HTTP status at all (a real
 * thrown bug) - is.
 */
function recordNestException(exception: any, host: any): void {
  if (!exception || !host || typeof host.switchToHttp !== "function") return;
  const req = host.switchToHttp()?.getRequest?.();
  if (!req) return;

  const status =
    typeof exception.getStatus === "function" ? exception.getStatus() : undefined;
  if (typeof status === "number" && status < 500) return;

  const error = {
    message: String(exception?.message ?? exception),
    stack: exception?.stack,
    name: exception?.name,
  };

  const log =
    (req as any).__POLARTRACE_REQUEST_LOG__ ??
    (req as any).raw?.__POLARTRACE_REQUEST_LOG__;
  if (log) {
    if (!log.error) log.error = error;
    return;
  }
  const state =
    (req as any).__POLARTRACE_FASTIFY_STATE__ ??
    (req as any).raw?.__POLARTRACE_FASTIFY_STATE__;
  if (state && !state.error) state.error = error;
}

/**
 * NestJS support. Request logs, traces, console capture and host metrics all
 * come from the underlying Express/Fastify hooks (Nest creates those apps
 * through the same patched factories) - but ERRORS never do: Nest's exception
 * layer catches every route exception and writes the response itself, so
 * nothing reaches Express's error middleware chain or Fastify's onError hook.
 *
 * `BaseExceptionFilter.catch` is the single funnel: the router's built-in
 * ExceptionsHandler extends it, and user filters created with
 * `extends BaseExceptionFilter` delegate to it via super.catch(). Patching it
 * records the exception onto the request log before Nest responds. (A custom
 * filter that fully handles an exception without calling super.catch() opted
 * out of default handling - the agent respects that and stays out.)
 */
function autoAttachNestErrorHook(): void {
  onModuleLoad("@nestjs/core", (module: any) => {
    const FilterClass = module?.BaseExceptionFilter;
    if (
      !FilterClass?.prototype ||
      typeof FilterClass.prototype.catch !== "function" ||
      FilterClass.__POLARTRACE_PATCHED__
    ) {
      return;
    }
    FilterClass.__POLARTRACE_PATCHED__ = true;

    const originalCatch = FilterClass.prototype.catch;
    FilterClass.prototype.catch = function (this: any, exception: any, host: any) {
      try {
        recordNestException(exception, host);
      } catch {
        // Capture must never break Nest's own error handling
      }
      return originalCatch.call(this, exception, host);
    };
  });
}

// Export PolarTrace class for testing purposes
// In production, users should use auto-initialization via -r polartrace
export { PolarTrace };

// Note: PolarTrace class is exported primarily for testing
// In production, the agent is used via auto-initialization with environment variables
// Direct instantiation is supported but auto-init is recommended

// Auto-enable instrumentation when loaded via -r polartrace
// This allows users to run: node -r polartrace server.js
// The register code will execute automatically when the module is loaded
import "./register";

// Auto-init the agent when loaded via -r polartrace (if env vars are present)
autoInitFromEnv();
