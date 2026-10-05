import { AGENT_VERSION } from "../version";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { FastifyInstrumentation } from "@opentelemetry/instrumentation-fastify";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { SpanStatusCode, SpanKind } from "@opentelemetry/api";
import { SpanProcessor, ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { randomUUID as uuid } from "crypto";
import { diagnostics } from "../diagnostics";

/**
 * The OpenTelemetry HTTP semantic conventions were stabilised: `http.method`
 * became `http.request.method`, `http.status_code` became
 * `http.response.status_code`, `http.url`/`http.target` became `url.*`, and the
 * `net.*` family became `network.*`/`server.*`/`client.*`.
 *
 * The collector, and this agent's own span routing, read the legacy names. Rather
 * than let an SDK upgrade change the wire format under the collector - or silently
 * break agent-span exclusion, which would make the agent trace its own traffic -
 * the legacy keys are derived back from the stable ones. Both sets are emitted, so
 * new consumers work and old ones keep working.
 */
function withLegacyHttpAttributes(
  attributes: Record<string, any>,
): Record<string, any> {
  const out = { ...attributes };
  const has = (key: string) => out[key] !== undefined;
  const backfill = (legacy: string, value: any) => {
    if (value !== undefined && !has(legacy)) out[legacy] = value;
  };

  backfill("http.method", out["http.request.method"]);
  backfill("http.status_code", out["http.response.status_code"]);
  backfill("http.scheme", out["url.scheme"]);
  backfill("http.user_agent", out["user_agent.original"]);
  backfill("net.peer.ip", out["network.peer.address"]);
  backfill("net.peer.port", out["network.peer.port"]);
  backfill("net.host.name", out["server.address"]);
  backfill("net.host.port", out["server.port"]);
  backfill("http.flavor", out["network.protocol.version"]);

  const path = out["url.path"];
  const query = out["url.query"];
  if (typeof path === "string") {
    backfill("http.target", query ? `${path}?${query}` : path);
  }

  const host =
    out["server.address"] !== undefined && out["server.port"] !== undefined
      ? `${out["server.address"]}:${out["server.port"]}`
      : out["server.address"];
  backfill("http.host", host);

  if (out["url.full"] !== undefined) {
    backfill("http.url", out["url.full"]);
  } else if (
    typeof out["url.scheme"] === "string" &&
    host !== undefined &&
    typeof path === "string"
  ) {
    backfill("http.url", `${out["url.scheme"]}://${host}${path}`);
  }

  return out;
}

export interface TraceSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  kind: string;
  startTime: number;
  endTime: number;
  duration: number;
  status: {
    code: string;
    message?: string;
  };
  attributes: Record<string, any>;
  events?: Array<{
    name: string;
    timestamp: number;
    attributes?: Record<string, any>;
  }>;
  links?: Array<{
    traceId: string;
    spanId: string;
  }>;
  resource?: Record<string, any>;
}

export interface TraceMonitorConfig {
  serviceName: string;
  serviceVersion?: string;
  onSpan: (span: TraceSpan) => void;
  excludeAgentSpans?: boolean;
  /** Mirrors PolarTraceConfig.recordUnmatchedRoutes - keep 404s that matched no route. */
  recordUnmatchedRoutes?: boolean;
  agentEndpoints?: string[];
  sharedSpanIdMap?: Map<string, string>; // Optional shared span ID mapping for consistency across spans
}

/**
 * Custom SpanProcessor that captures spans and sends them via callback
 */
class CustomSpanProcessor implements SpanProcessor {
  private config: TraceMonitorConfig;
  // Map to store 16-character span IDs to 32-character UUID conversions
  // This maintains parent-child relationships when converting IDs
  // Uses shared map if provided, otherwise creates its own
  private spanIdMap: Map<string, string>;

  constructor(config: TraceMonitorConfig) {
    this.config = config;
    // Use shared spanIdMap if provided, otherwise create a new one
    this.spanIdMap = config.sharedSpanIdMap || new Map<string, string>();
  }

  /**
   * OpenTelemetry represents "no parent" with absent context, but some exporters
   * still emit an all-zero span id. Treat those as root (no parent) so spans are
   * not wrongly attached to a synthetic parent UUID.
   */
  private static isUnsetOtlpSpanId(id: string | undefined): boolean {
    if (id === undefined || id === null) {
      return true;
    }
    const t = String(id).trim().toLowerCase();
    return t.length === 0 || /^0+$/.test(t);
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

  onStart(span: ReadableSpan): void {
    // Span started, we'll capture it on end
  }

  onEnd(span: ReadableSpan): void {
    try {
      // MongoDB span filtering - MongoDB spans are handled by custom SDK
      if (this.isMongoDbSpanFromOTEL(span)) {
        return;
      }

      // Redis span filtering - Redis spans are handled by custom SDK
      if (this.isRedisSpanFromOTEL(span)) {
        return;
      }

      // Postgres span filtering - Postgres spans are handled by custom SDK
      if (this.isPostgresSpanFromOTEL(span)) {
        return;
      }

      // Filter out low-level internal spans (TCP connections, DNS lookups, etc.)
      if (this.isInternalNoiseSpan(span)) {
        return;
      }

      // Filter out agent-level spans if configured
      if (this.config.excludeAgentSpans && this.isAgentSpan(span)) {
        return;
      }

      // Drop traces for paths the application does not serve, so a scanned
      // service does not accumulate thousands of traces for routes it has
      // never had. Matches the request-log rule in index.ts.
      if (this.isUnmatchedRouteSpan(span)) {
        return;
      }

      // Filter out Express middleware / request-handler spans to reduce noise
      // We keep only the top-level HTTP SERVER span (GET /route) plus DB/CLIENT spans.
      if (this.isExpressMiddlewareSpan(span)) {
        return;
      }

      const startTimeNs = span.startTime[0] * 1000000000 + span.startTime[1];
      const endTimeNs = span.endTime[0] * 1000000000 + span.endTime[1];
      const durationMs = (endTimeNs - startTimeNs) / 1000000;

      // Extract parent span ID from span context. The shape moved in the SDK
      // 2.x line - `parentSpanId` became `parentSpanContext` - so read both
      // rather than pinning the agent to one SDK generation.
      const rawParentSpanId =
        (span as any).parentSpanContext?.spanId ||
        (span as any).parentSpanId ||
        (span as any).parentContext?.spanId ||
        undefined;

      // Get original span IDs from OpenTelemetry (16-character hex strings)
      const originalSpanId = span.spanContext().spanId;

      // Convert span IDs to UUIDs (32-character hex strings)
      const spanIdUUID = this.convertSpanIdToUUID(originalSpanId);
      const parentSpanIdUUID = CustomSpanProcessor.isUnsetOtlpSpanId(rawParentSpanId)
          ? undefined
          : this.convertSpanIdToUUID(rawParentSpanId);

      const traceSpan: TraceSpan = {
        traceId: span.spanContext().traceId,
        spanId: spanIdUUID, // Converted to 32-character UUID
        parentSpanId: parentSpanIdUUID, // Converted to 32-character UUID, maintains parent-child relationship
        name: span.name,
        kind: this.getSpanKind(span.kind),
        startTime: startTimeNs,
        endTime: endTimeNs,
        duration: durationMs,
        status: {
          code: span.status.code === SpanStatusCode.ERROR ? "ERROR" : "OK",
          message: span.status.message,
        },
        attributes: withLegacyHttpAttributes(
          this.sanitizeAttributes(span.attributes),
        ),
        events: span.events?.map((event: any) => ({
          name: event.name,
          timestamp: event.time[0] * 1000000000 + event.time[1],
          attributes: this.sanitizeAttributes(event.attributes || {}),
        })),
        links: span.links?.map((link: any) => ({
          traceId: link.context.traceId,
          spanId: this.convertSpanIdToUUID(link.context.spanId), // Convert linked span IDs to UUIDs
        })),
        resource: span.resource?.attributes
          ? this.sanitizeAttributes(span.resource.attributes)
          : undefined,
      };

      const route =
        traceSpan.attributes?.["http.route"] ||
        traceSpan.attributes?.["http.target"];
      diagnostics.debug(
        `span ${traceSpan.name} [${traceSpan.kind}] ` +
          `trace=${traceSpan.traceId.substring(0, 8)} ` +
          `span=${traceSpan.spanId.substring(0, 8)} ` +
          `parent=${traceSpan.parentSpanId?.substring(0, 8) ?? "root"} ` +
          `${traceSpan.duration.toFixed(2)}ms` +
          (route ? ` ${route}` : ""),
      );

      this.config.onSpan(traceSpan);
    } catch (error: any) {
      diagnostics.debug(`error processing span: ${error.message}`);
    }
  }

  /**
   * Check if a span from OTEL is a MongoDB span
   * MongoDB spans are handled by custom SDK and should be filtered out
   */
  private isMongoDbSpanFromOTEL(span: ReadableSpan): boolean {
    const attributes = span.attributes || {};
    const dbSystem = attributes["db.system"];
    const spanName = span.name?.toLowerCase() || "";
    return (
      dbSystem === "mongodb" ||
      spanName.includes("mongodb") ||
      spanName.includes("mongoose")
    );
  }

  /**
   * Check if a span from OTEL is a Redis span.
   * Redis spans are handled by custom SDK and should be filtered out.
   */
  private isRedisSpanFromOTEL(span: ReadableSpan): boolean {
    const attributes = span.attributes || {};
    const dbSystem = attributes["db.system"];
    const spanName = span.name?.toLowerCase() || "";
    return (
      dbSystem === "redis" ||
      spanName.startsWith("redis") ||
      spanName.startsWith("ioredis")
    );
  }

  private isPostgresSpanFromOTEL(span: ReadableSpan): boolean {
    const attributes = span.attributes || {};
    const dbSystem = attributes["db.system"];
    const spanName = span.name?.toLowerCase() || "";
    return (
      dbSystem === "postgresql" ||
      spanName.startsWith("pg.") ||
      spanName.startsWith("pg-pool")
    );
  }

  /**
   * A server span for a path that matched no route and answered 404.
   *
   * `http.route` is set by the framework instrumentation only when a route
   * matched, so its absence on a 404 SERVER span marks a probe for an endpoint
   * this application does not have. Both signals are required: a matched route
   * may legitimately return 404, and a routeless 200 (a static file) is real
   * traffic.
   */
  private isUnmatchedRouteSpan(span: ReadableSpan): boolean {
    if (this.config.recordUnmatchedRoutes) return false;
    if (span.kind !== SpanKind.SERVER) return false;

    const attributes = span.attributes || {};
    const status = Number(
      attributes["http.status_code"] ?? attributes["http.response.status_code"],
    );
    if (status !== 404) return false;

    const route = attributes["http.route"];
    return route === undefined || route === null || route === "";
  }

  private isInternalNoiseSpan(span: ReadableSpan): boolean {
    const spanName = span.name?.toLowerCase() || "";
    const attributes = span.attributes || {};

    // Filter out low-level internal spans that are not useful for application tracing
    const noiseSpanNames = [
      "tcp.connect",
      "tcp.socket",
      "dns.lookup",
      "net.connect",
      "socket.connect",
      "connect",
    ];

    // Check if span name matches any noise patterns
    if (noiseSpanNames.some((noise) => spanName.includes(noise))) {
      return true;
    }

    // Filter out INTERNAL spans with no meaningful attributes (likely internal networking)
    if (span.kind === SpanKind.INTERNAL) {
      // If it's an INTERNAL span with only network-level attributes, filter it out
      const hasOnlyNetworkAttrs = Object.keys(attributes).every(
        (key) =>
          key.startsWith("net.") ||
          key.startsWith("tcp.") ||
          key.startsWith("dns.") ||
          key === "peer.address" ||
          key === "peer.port",
      );

      // Keep INTERNAL spans that have meaningful application-level attributes
      const hasMeaningfulAttrs = Object.keys(attributes).some(
        (key) =>
          key.startsWith("http.") ||
          key.startsWith("db.") ||
          key.startsWith("rpc.") ||
          key.startsWith("messaging.") ||
          key === "service.name" ||
          key === "operation.name",
      );

      // Filter out if it only has network attributes and no meaningful ones
      if (hasOnlyNetworkAttrs && !hasMeaningfulAttrs) {
        return true;
      }
    }

    return false;
  }

  /**
   * True only for the agent's own outbound calls to its collector.
   *
   * This used to substring-match the URL of every span against patterns including bare
   * "/log" and "/logs", which swallowed the customer's own routes: an application with
   * /login, /logout or /api/logs produced request logs carrying a trace id whose trace had
   * no spans at all, so those routes were missing from the waterfall, from latency
   * breakdowns and from throughput - while looking, from the log side, as if they were
   * traced. Two rules keep it honest now: an inbound SERVER span is by definition the
   * application's own traffic and can never be agent chatter, and the match is on the exact
   * URL (preferring the collector's own absolute address) rather than any substring of it.
   */
  private isAgentSpan(span: ReadableSpan): boolean {
    if (span.kind !== SpanKind.CLIENT) {
      return false;
    }

    const attributes = withLegacyHttpAttributes(span.attributes || {});
    const httpUrl = attributes["http.url"] || attributes["url"];

    if (!httpUrl || typeof httpUrl !== "string") {
      return false;
    }

    const agentEndpoints = this.config.agentEndpoints || [
      "/api/traces",
      "/api/log",
      "/api/logs",
      "/api/host-metrics",
      "/api/service/validate",
    ];

    const trimTrailingSlash = (value: string) => value.replace(/\/+$/, "") || "/";

    let pathname = httpUrl;
    let absolute = "";
    try {
      const parsed = new URL(httpUrl);
      pathname = parsed.pathname;
      absolute = `${parsed.origin}${parsed.pathname}`;
    } catch {
      // Relative URL - compare it as given.
    }

    // When the collector's absolute address is known, only calls to that exact address are
    // ours; a customer service that happens to expose the same path is left alone.
    const absoluteEndpoints = agentEndpoints.filter((endpoint) => /^https?:\/\//i.test(endpoint));
    if (absolute && absoluteEndpoints.length > 0) {
      return absoluteEndpoints.some(
        (endpoint) => trimTrailingSlash(absolute) === trimTrailingSlash(endpoint),
      );
    }

    return agentEndpoints
      .filter((endpoint) => !/^https?:\/\//i.test(endpoint))
      .some((endpoint) => trimTrailingSlash(pathname) === trimTrailingSlash(endpoint));
  }

  /**
   * Filter out spans generated by Express instrumentation for middleware layers
   * and request handlers (we keep the top-level SERVER span instead).
   */
  private isExpressMiddlewareSpan(span: ReadableSpan): boolean {
    if (span.kind !== SpanKind.INTERNAL) {
      return false;
    }

    const name = span.name || "";
    const lower = name.toLowerCase();

    return (
      lower.startsWith("middleware - ") ||
      lower.startsWith("request handler - ")
    );
  }

  private getSpanKind(kind: number): string {
    switch (kind) {
      case SpanKind.SERVER:
        return "SERVER";
      case SpanKind.CLIENT:
        return "CLIENT";
      case SpanKind.INTERNAL:
        return "INTERNAL";
      case SpanKind.PRODUCER:
        return "PRODUCER";
      case SpanKind.CONSUMER:
        return "CONSUMER";
      default:
        return "UNKNOWN";
    }
  }

  private sanitizeAttributes(
    attributes: Record<string, any>,
  ): Record<string, any> {
    const sanitized: Record<string, any> = {};
    const sensitiveFields = [
      "password",
      "token",
      "secret",
      "apiKey",
      "authorization",
      "creditCard",
      "ssn",
    ];

    for (const [key, value] of Object.entries(attributes)) {
      if (
        sensitiveFields.some((field) =>
          key.toLowerCase().includes(field.toLowerCase()),
        )
      ) {
        sanitized[key] = "[REDACTED]";
      } else {
        try {
          JSON.stringify(value);
          sanitized[key] = value;
        } catch {
          sanitized[key] = String(value);
        }
      }
    }

    return sanitized;
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }
}

export class TraceMonitor {
  private config: TraceMonitorConfig;
  private sdk?: NodeSDK;
  private isInitialized: boolean = false;

  constructor(config: TraceMonitorConfig) {
    this.config = config;
  }

  public get initialized(): boolean {
    return this.isInitialized;
  }

  public async initialize(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    try {
      const resource = resourceFromAttributes({
        "service.name": this.config.serviceName,
        "service.version": this.config.serviceVersion || AGENT_VERSION,
      });

      const spanProcessor = new CustomSpanProcessor(this.config);

      // Initialize SDK with auto-instrumentations.
      // Mongo + Redis instrumentation is handled by custom SDK (not OTEL default) so
      // both DB drivers emit through the same pipeline with consistent span shape.
      const instrumentations: Record<string, { enabled: boolean }> = {
        "@opentelemetry/instrumentation-fs": {
          enabled: false, // Disabled to reduce noise
        },
        "@opentelemetry/instrumentation-mongoose": {
          enabled: false, // Disabled - Using custom MongoDB instrumentation instead
        },
        "@opentelemetry/instrumentation-mongodb": {
          enabled: false, // Disabled - Using custom MongoDB instrumentation instead
        },
        "@opentelemetry/instrumentation-ioredis": {
          enabled: false, // Disabled - Using custom Redis instrumentation instead
        },
        "@opentelemetry/instrumentation-redis": {
          enabled: false, // Disabled - Using custom Redis instrumentation instead
        },
        "@opentelemetry/instrumentation-redis-4": {
          enabled: false, // Disabled - Using custom Redis instrumentation instead
        },
        "@opentelemetry/instrumentation-http": {
          enabled: true, // Enabled for HTTP request tracing
        },
        "@opentelemetry/instrumentation-express": {
          enabled: true, // Enabled for Express route tracing
        },
        "@opentelemetry/instrumentation-koa": {
          enabled: true, // Enabled for Koa route tracing (2.x and 3.x)
        },
      };

      // Pass our span processor to NodeSDK via spanProcessors
      // NodeSDK will create its own TracerProvider and add our processor to it
      this.sdk = new NodeSDK({
        resource,
        spanProcessors: [spanProcessor], // Use our custom span processor
        instrumentations: [
          getNodeAutoInstrumentations(instrumentations),
          // Fastify is no longer part of the auto-instrumentations bundle, so it
          // has to be registered explicitly - without it Fastify spans carry no
          // http.route and every endpoint collapses to a bare "GET".
          new FastifyInstrumentation(),
        ],
      });

      // CRITICAL: Start SDK synchronously - this patches HTTP/Express
      // MongoDB instrumentation is handled separately by custom SDK
      // NodeSDK.start() is synchronous, so this completes immediately
      this.sdk.start();

      // Mark as initialized immediately since SDK.start() is synchronous
      this.isInitialized = true;

      // Mark MongoDB instrumentation as enabled (custom MongoDB instrumentation)
      (globalThis as any).__POLARTRACE_MONGO_INSTRUMENTATION_ENABLED__ = true;

      diagnostics.debug(
        "OpenTelemetry SDK started - http/express tracing active",
      );
    } catch (error: any) {
      throw new Error(
        `Failed to initialize trace monitoring: ${error.message}`,
      );
    }
  }

  public destroy(): void {
    if (this.sdk) {
      this.sdk.shutdown().catch((err: any) => {
        diagnostics.debug(`error shutting down OpenTelemetry SDK: ${err}`);
      });
    }
    this.isInitialized = false;
  }
}
