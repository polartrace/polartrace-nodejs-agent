import { Request, Response, NextFunction } from "express";
import { trace as otelTrace } from "@opentelemetry/api";
import { TraceMonitor, TraceSpan } from "./trace";
import * as fs from "fs";
import * as path from "path";
import { v4 as uuid } from "uuid";

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

// Host (CPU / memory / event-loop) metrics
import {
  HostMetricsMonitor,
  HostMetricSample,
} from "./metrics/host-metrics-monitor";

export interface PolarTraceConfig {
  apiKey: string;
  serviceName: string;
  endpoint?: string;
  enableConsoleLog?: boolean;
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
  private config: Required<Omit<PolarTraceConfig, "endpoint" | "mongoose">> & {
    endpoint?: string;
    mongoose?: any;
  };
  private connectionStatus: ConnectionStatus = "pending";
  private connectionError?: string;
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
  private readonly FLUSH_INTERVAL = 10000; // 10 seconds
  private readonly API_TIMEOUT = 10000; // 10 seconds
  private logFilePath: string;
  private logFileStream?: fs.WriteStream;

  // Trace grouping: cache spans by traceId until parent span completes
  private traceCache: Map<string, TraceSpanData[]> = new Map();
  private parentSpanTracker: Map<string, string> = new Map(); // traceId -> parentSpanId
  private traceTimestamps: Map<string, number> = new Map(); // traceId -> creation timestamp
  private readonly TRACE_CACHE_TIMEOUT = 60000; // 60 seconds - cleanup orphaned traces

  // Shared span ID mapping: maps 16-character OpenTelemetry span IDs to 32-character UUIDs
  // This maintains parent-child relationships across trace-monitor and MongoDB spans
  private spanIdMap: Map<string, string> = new Map();

  constructor(config: PolarTraceConfig) {
    const defaultEndpoint = "https://collector.polartrace.com/api/log";

    this.config = {
      enableConsoleLog: false,
      captureHeaders: true,
      captureBody: true,
      captureQuery: true,
      captureConsoleLogs: true,
      enableMongoSpanCollection: true,
      enableRedisSpanCollection: true,
      enableHostMetrics: true,
      endpoint: defaultEndpoint,
      ...config,
    };

    // Initialize file logging
    this.logFilePath = path.join(process.cwd(), "polartrace_agent.log");
    this.initializeFileLogging();

    // Validate serviceName
    if (!this.config.serviceName) {
      throw new Error("PolarTrace: serviceName is required");
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

  private initializeFileLogging(): void {
    try {
      // Create write stream in append mode
      this.logFileStream = fs.createWriteStream(this.logFilePath, {
        flags: "a",
      });
    } catch (error: any) {
      // Silent fail - file logging is not critical
      // If file can't be opened, just continue without file logging
    }
  }

  /**
   * Write a message to the log file
   */
  private writeToLogFile(message: string): void {
    if (this.logFileStream) {
      try {
        const timestamp = new Date().toISOString();
        this.logFileStream.write(`[${timestamp}] ${message}\n`);
      } catch (error) {
        // Silent fail - if write fails, just continue
      }
    }
  }

  /**
   * POST JSON to an endpoint using native fetch with a timeout.
   * Throws on network errors, abort/timeout, and HTTP status >= 500.
   * Returns {status, data} for any status < 500 (callers handle 4xx).
   */
  private async postJson(
    url: string,
    body: any,
  ): Promise<{ status: number; data: any }> {
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

      return { status: res.status, data };
    } finally {
      clearTimeout(timeoutId);
    }
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
      throw new Error("PolarTrace: API key is required");
    }

    if (this.config.apiKey.length < 10) {
      throw new Error("PolarTrace: Invalid API key format");
    }
  }

  private setupFlushTimer(): void {
    // Set up flush timer to send logs every 10 seconds
    if (this.config.endpoint) {
      this.flushTimer = setInterval(() => {
        this.flushLogs();
      }, this.FLUSH_INTERVAL);

      // Set up trace flush timer (independent process)
      this.traceFlushTimer = setInterval(() => {
        this.flushTraces();
      }, this.FLUSH_INTERVAL);

      // Set up host metrics flush timer (independent process)
      this.hostMetricsFlushTimer = setInterval(() => {
        this.flushHostMetrics();
      }, this.FLUSH_INTERVAL);
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
          this.writeToLogFile(
            `PolarTrace: Host metrics sampling error: ${err.message}`,
          );
        },
      });
      this.hostMetricsMonitor.initialize();

      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          "PolarTrace: Host metrics monitoring started (CPU/memory/event-loop)",
        );
      }
    } catch (err: any) {
      // Don't throw - host metrics are optional and must never break the host app.
      this.writeToLogFile(
        `PolarTrace: Failed to initialize host metrics monitoring: ${err.message}`,
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
          if (this.config.enableConsoleLog) {
            this.writeToLogFile("PolarTrace: Connection validated successfully");
          }
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

        if (this.config.enableConsoleLog) {
          this.writeToLogFile(
            `PolarTrace: validate attempt ${attempt + 1}/${backoffsMs.length} failed - ${reason}`,
          );
        }

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
   * Show connection status summary in a grouped console output
   */
  private showConnectionStatus(): void {
    const httpOk = !!this.traceMonitor?.initialized;
    const mongoOk =
      !this.config.enableMongoSpanCollection ||
      !!(globalThis as any).__POLARTRACE_MONGO_INSTRUMENTATION_ENABLED__;
    const redisOk =
      !this.config.enableRedisSpanCollection ||
      !!(globalThis as any).__POLARTRACE_REDIS_INSTRUMENTATION_ENABLED__;
    const connectionOk = this.connectionStatus === "connected";
    const connectionFailed = this.connectionStatus === "failed";

    // Don't show status if connection validation is still pending
    if (this.connectionStatus === "pending") {
      return;
    }

    const httpStatus = httpOk ? "☑️ OK" : "❌ ERROR";
    const mongoStatus = mongoOk ? "☑️ OK" : "❌ ERROR";
    const redisStatus = redisOk ? "☑️ OK" : "❌ ERROR";
    const connectionStatus = connectionOk ? "☑️ OK" : "❌ ERROR";

    const allOk = httpOk && mongoOk && redisOk && connectionOk;

    if (allOk) {
      // Simple one-line success message
      const message = "Polartrace agent status: ☑️ OK";
      this.writeToLogFile(message);
      return;
    }

    // At least one error - show detailed table with only failing agents
    // Build log message for file
    const connectionColWidth = 18;
    const statusColWidth = 12;
    const reasonColWidth = 60;
    const totalWidth =
      connectionColWidth + statusColWidth + reasonColWidth + 20; // +8 for borders and spaces

    // Build separator line
    const separator = "─".repeat(totalWidth);

    // Build status message for file
    let statusMessage = "Polartrace agent status\n";
    statusMessage += separator + "\n";
    statusMessage += `│ ${"Connection".padEnd(connectionColWidth)} │ ${"Status".padEnd(statusColWidth)} │ ${"Reason".padEnd(reasonColWidth)} │\n`;
    statusMessage += separator + "\n";

    // Data rows
    if (connectionFailed) {
      const connectionName = "Agent".padEnd(connectionColWidth);
      const statusDisplay = connectionStatus.padEnd(statusColWidth - 1);
      const reasonText = this.connectionError
        ? this.connectionError.substring(0, reasonColWidth)
        : "";
      const reasonDisplay = reasonText.padEnd(reasonColWidth);
      statusMessage += `│ ${connectionName} │ ${statusDisplay} │ ${reasonDisplay} │\n`;
    }
    if (!httpOk) {
      const connectionName = "HTTP Agent".padEnd(connectionColWidth);
      const statusDisplay = httpStatus.padEnd(statusColWidth);
      const reasonDisplay = "-".padEnd(reasonColWidth);
      statusMessage += `│ ${connectionName} │ ${statusDisplay} │ ${reasonDisplay} │\n`;
    }
    if (!mongoOk) {
      const connectionName = "Mongo Agent".padEnd(connectionColWidth);
      const statusDisplay = mongoStatus.padEnd(statusColWidth);
      const reasonDisplay = "-".padEnd(reasonColWidth);
      statusMessage += `│ ${connectionName} │ ${statusDisplay} │ ${reasonDisplay} │\n`;
    }
    if (!redisOk) {
      const connectionName = "Redis Agent".padEnd(connectionColWidth);
      const statusDisplay = redisStatus.padEnd(statusColWidth);
      const reasonDisplay = "-".padEnd(reasonColWidth);
      statusMessage += `│ ${connectionName} │ ${statusDisplay} │ ${reasonDisplay} │\n`;
    }

    statusMessage += separator + "\n";

    // Write to log file only (not to console)
    this.writeToLogFile(statusMessage.trim());
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

        // Add common agent endpoint patterns
        agentEndpointPatterns.push(
          "/api/traces",
          "/api/log",
          "/api/logs",
          "/api/host-metrics",
          "/api/service/validate",
          "/traces",
          "/log",
          "/logs",
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
        serviceVersion: "1.0.0",
        enableConsoleLog: this.config.enableConsoleLog,
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
        this.writeToLogFile(
          `PolarTrace: Failed to initialize trace monitoring: ${err}`,
        );
      });

      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          "PolarTrace: Trace monitoring initialization started (SDK will be ready before mongoose loads)",
        );
      }

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
    } catch (error: any) {
      this.writeToLogFile(
        `PolarTrace: Failed to initialize trace monitoring: ${error.message}`,
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
      span.attributes?.["http.method"] !== undefined
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

    if (this.config.enableConsoleLog) {
      this.writeToLogFile(
        `PolarTrace: Flushed trace ${traceId.substring(0, 8)}... with ${spans.length} spans (parent completed)`,
      );
    }

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
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: Cleaning up orphaned trace ${traceId.substring(0, 8)}... (timeout)`,
        );
      }

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

      // Optionally capture additional data
      if (this.config.captureHeaders) {
        requestLog.headers = req.headers as Record<
          string,
          string | string[] | undefined
        >;
      }

      if (this.config.captureQuery && Object.keys(req.query).length > 0) {
        requestLog.query = req.query;
      }

      if (this.config.captureBody && req.body) {
        // Sanitize sensitive data
        requestLog.body = this.sanitizeBody(req.body);
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

      next();
    };
  }

  private handleResponse(
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

  private sanitizeBody(body: any): any {
    if (typeof body !== "object" || body === null) {
      return body;
    }

    const sensitiveFields = [
      "password",
      "token",
      "secret",
      "apiKey",
      "authorization",
      "creditCard",
      "ssn",
    ];
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
        sanitized[key] = this.sanitizeBody(sanitized[key]);
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
    if (this.config.enableConsoleLog) {
      this.writeToLogFile(
        `PolarTrace: Log queued. Queue size: ${this.logQueue.length}`,
      );
    }
  }

  private async flushLogs(): Promise<void> {
    // Only flush if there are logs in the queue
    if (this.isFlushing || this.logQueue.length === 0) {
      return;
    }

    this.isFlushing = true;
    const queueLength = this.logQueue.length;
    // Create a copy of logs to send, but DON'T remove from queue yet
    const logsToSend = this.logQueue
      .slice(0, queueLength)
      .map((item) => item.log);

    if (this.config.enableConsoleLog) {
      this.writeToLogFile(
        `PolarTrace: Attempting to send ${logsToSend.length} logs...`,
      );
    }

    try {
      // Send logs to API
      await this.sendLogsToAPI(logsToSend);

      // Only remove from queue AFTER successful send
      this.logQueue.splice(0, queueLength);
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: Successfully sent ${logsToSend.length} logs and removed from queue`,
        );
      }
    } catch (error: any) {
      // If sending fails, keep logs in queue for retry
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: Failed to send logs to API: ${error?.message}`,
        );
        this.writeToLogFile(
          `PolarTrace: Keeping ${queueLength} logs in queue for retry`,
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
      if (this.config.enableConsoleLog) {
        if (error?.response) {
          this.writeToLogFile(
            `ERROR: PolarTrace API responded with error: ${error.response.status} ${JSON.stringify(error.response.data, null, 2)}`,
          );
        } else if (error?.name === "AbortError") {
          this.writeToLogFile("ERROR: No response received");
        } else {
          this.writeToLogFile(`ERROR: Failed to send logs ${error?.message}`);
        }
      }
      throw error;
    }
  }

  private async flushTraces(): Promise<void> {
    // Cleanup old traces before flushing
    this.cleanupOldTraces();

    if (this.config.enableConsoleLog) {
      this.writeToLogFile(
        `PolarTrace: flushTraces called - queue size: ${this.traceQueue.length}, isFlushing: ${this.isTraceFlushing}`,
      );
    }

    if (this.isTraceFlushing || this.traceQueue.length === 0) {
      if (this.config.enableConsoleLog) {
        if (this.isTraceFlushing) {
          this.writeToLogFile(
            "PolarTrace: Trace flush skipped - already flushing",
          );
        } else {
          this.writeToLogFile(
            "PolarTrace: Trace flush skipped - queue is empty",
          );
        }
      }
      return;
    }

    this.isTraceFlushing = true;
    const queueLength = this.traceQueue.length;

    // Create a copy of spans to send (don't modify queue yet)
    const spansToSend = this.traceQueue
      .slice(0, queueLength)
      .map((item) => item.span);

    if (this.config.enableConsoleLog) {
      this.writeToLogFile(
        `PolarTrace: Attempting to send ${spansToSend.length} trace spans...`,
      );
      this.writeToLogFile(
        `PolarTrace: Current queue size before send: ${this.traceQueue.length}`,
      );
    }

    try {
      const response = await this.sendTracesToAPI(spansToSend);

      // Only remove spans from queue if API call was successful (2xx status)
      const status = response?.status;
      if (response && status >= 200 && status < 300) {
        // Successfully sent - remove spans from queue
        // Use splice to remove exactly queueLength items from the beginning
        const removed = this.traceQueue.splice(0, queueLength);
        const removedCount = removed.length;

        if (this.config.enableConsoleLog) {
          this.writeToLogFile(
            `PolarTrace: Successfully sent ${spansToSend.length} trace spans (status: ${status})`,
          );
          this.writeToLogFile(
            `PolarTrace: Removed ${removedCount} spans from queue`,
          );
          this.writeToLogFile(
            `PolarTrace: Remaining queue size: ${this.traceQueue.length}`,
          );

          // Verify removal
          if (removedCount !== queueLength) {
            this.writeToLogFile(
              `PolarTrace: ⚠️ Warning: Expected to remove ${queueLength} spans but removed ${removedCount}`,
            );
          }
        }
      } else {
        // Non-2xx response - don't remove from queue, will retry
        if (this.config.enableConsoleLog) {
          this.writeToLogFile(
            `PolarTrace: ⚠️ API returned non-success status ${status}, keeping ${queueLength} spans in queue for retry`,
          );
        }
        // Throw error to prevent removal from queue
        throw new Error(`API returned status ${status}`);
      }
    } catch (error: any) {
      // Error occurred - don't remove spans from queue, will retry on next flush
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: ❌ Failed to send trace spans to API: ${error?.message}`,
        );
        this.writeToLogFile(
          `PolarTrace: 🔄 Keeping ${queueLength} trace spans in queue for retry`,
        );
        this.writeToLogFile(
          `PolarTrace: Current queue size: ${this.traceQueue.length}`,
        );
      }
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

    if (this.config.enableConsoleLog) {
      this.writeToLogFile(
        `PolarTrace: Sending traces to endpoint: ${tracesEndpoint}`,
      );
    }

    try {
      const response = await this.postJson(tracesEndpoint, { spans });

      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: API response received - status: ${response.status}`,
        );
      }

      return response;
    } catch (error: any) {
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: Failed to send trace spans: ${error?.message}`,
        );
        if (error?.response) {
          this.writeToLogFile(
            `PolarTrace: Response status: ${error.response.status}`,
          );
          this.writeToLogFile(
            `PolarTrace: Response data: ${JSON.stringify(error.response.data)}`,
          );
        }
      }
      throw error;
    }
  }

  /**
   * Flush queued host-metric samples to the collector. On any failure the
   * batch is kept for retry (same semantics as logs and traces).
   */
  private async flushHostMetrics(): Promise<void> {
    if (this.isHostMetricsFlushing || this.hostMetricsQueue.length === 0) {
      return;
    }

    this.isHostMetricsFlushing = true;
    const queueLength = this.hostMetricsQueue.length;
    const samplesToSend = this.hostMetricsQueue
      .slice(0, queueLength)
      .map((item) => item.sample);

    try {
      await this.sendHostMetricsToAPI(samplesToSend);
      this.hostMetricsQueue.splice(0, queueLength);
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: Successfully sent ${samplesToSend.length} host metric samples`,
        );
      }
    } catch (error: any) {
      if (this.config.enableConsoleLog) {
        this.writeToLogFile(
          `PolarTrace: Failed to send host metrics: ${error?.message}`,
        );
        this.writeToLogFile(
          `PolarTrace: Keeping ${queueLength} host metric samples in queue for retry`,
        );
      }
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
  ): Promise<void> {
    if (!this.config.endpoint) {
      throw new Error("Endpoint not configured");
    }

    const baseUrl = this.getBaseUrl();
    const hostMetricsEndpoint = `${baseUrl}/api/host-metrics`.replace(
      /([^:]\/)\/+/g,
      "$1",
    );

    await this.postJson(hostMetricsEndpoint, { samples });
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
        const errorMsg = `PolarTrace: Error flushing logs on destroy: ${err}`;
        this.writeToLogFile(errorMsg);
      });
    }

    if (this.traceQueue.length > 0) {
      this.flushTraces().catch((err) => {
        const errorMsg = `PolarTrace: Error flushing traces on destroy: ${err}`;
        this.writeToLogFile(errorMsg);
      });
    }

    if (this.hostMetricsQueue.length > 0) {
      this.flushHostMetrics().catch((err) => {
        const errorMsg = `PolarTrace: Error flushing host metrics on destroy: ${err}`;
        this.writeToLogFile(errorMsg);
      });
    }

    // Destroy monitors
    if (this.traceMonitor) {
      this.traceMonitor.destroy();
    }
    if (this.hostMetricsMonitor) {
      this.hostMetricsMonitor.destroy();
    }

    // Close log file stream
    if (this.logFileStream) {
      try {
        this.logFileStream.end();
      } catch (error) {
        // Silent fail
      }
    }
  }
}

/**
 * Auto-initialize PolarTrace when the module is preloaded via `node -r polartrace`.
 * This uses environment variables so users don't have to change their application code.
 *
 * Supported environment variables:
 * - POLARTRACE_APP_NAME                         -> serviceName
 * - POLARTRACE_LICENSE_KEY                      -> apiKey
 * - POLARTRACE_ENDPOINT                         -> override the collector URL
 *                                                  (default: https://collector.polartrace.com/api/log)
 * - POLARTRACE_ENABLE_CONSOLE_LOG               -> enableConsoleLog ("1"/"true" to enable)
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

  const enableConsoleLog =
    process.env.POLARTRACE_ENABLE_CONSOLE_LOG === "1" ||
    process.env.POLARTRACE_ENABLE_CONSOLE_LOG?.toLowerCase() === "true";

  // Host metrics are on by default; opt-out via POLARTRACE_DISABLE_HOST_METRICS=1
  const disableHostMetricsEnv = process.env.POLARTRACE_DISABLE_HOST_METRICS;
  const enableHostMetrics = !(
    disableHostMetricsEnv === "1" ||
    disableHostMetricsEnv?.toLowerCase() === "true"
  );

  // DB span collection is on by default; opt-out via
  //   POLARTRACE_DISABLE_MONGO_SPANS=1 / POLARTRACE_DISABLE_REDIS_SPANS=1
  const disableMongoEnv = process.env.POLARTRACE_DISABLE_MONGO_SPANS;
  const enableMongoSpanCollection = !(
    disableMongoEnv === "1" || disableMongoEnv?.toLowerCase() === "true"
  );

  const disableRedisEnv = process.env.POLARTRACE_DISABLE_REDIS_SPANS;
  const enableRedisSpanCollection = !(
    disableRedisEnv === "1" || disableRedisEnv?.toLowerCase() === "true"
  );

  const endpoint = process.env.POLARTRACE_ENDPOINT?.trim() || undefined;

  const agent = new PolarTrace({
    apiKey,
    serviceName,
    enableConsoleLog,
    enableHostMetrics,
    enableMongoSpanCollection,
    enableRedisSpanCollection,
    ...(endpoint ? { endpoint } : {}),
  });

  globalObj.__POLARTRACE_AGENT__ = agent;

  // Automatically attach middleware to Express apps
  autoAttachExpressMiddleware(agent);
}

/**
 * Automatically attach PolarTrace middleware to Express apps when they're created.
 * This patches Express so that any app created will automatically have our middleware attached.
 */
function autoAttachExpressMiddleware(agent: PolarTrace): void {
  try {
    // Use a require hook to intercept Express when it's loaded
    const Module = require("module");
    const originalRequire = Module.prototype.require;

    Module.prototype.require = function (this: any, id: string) {
      const module = originalRequire.apply(this, arguments);

      // When Express is required, patch the express() function
      if (id === "express" && typeof module === "function") {
        const originalExpress = module;
        const patchedExpress = function (this: any) {
          const app = originalExpress.apply(this, arguments);

          // Check if middleware is already attached (avoid double attachment)
          if (!(app as any).__POLARTRACE_MIDDLEWARE_ATTACHED__) {
            (app as any).__POLARTRACE_MIDDLEWARE_ATTACHED__ = true;

            // Immediately attach middleware to the app
            // Since we're loaded via -r polartrace, we run before user code,
            // so this middleware will be early in the stack
            try {
              app.use(agent.middleware());
            } catch (err: any) {
              // Silent fail - middleware attachment is best effort
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
      }

      return module;
    };
  } catch (error: any) {
    // Silent fail - middleware attachment is best effort
  }
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
