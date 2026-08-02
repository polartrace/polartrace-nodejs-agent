import Module from "module";
import { Instrumentation } from "./instrumentation";
import { trace } from "@opentelemetry/api";

export interface PostgresSdkSpan {
  name: string;
  startTime: number;
  endTime?: number;
  durationMs?: number;
  attributes: {
    /** SQL command keyword, e.g. "SELECT", "INSERT", "UPDATE". */
    operation?: string;
    /** Primary table the statement touches (best-effort parse). */
    table?: string;
    /** The SQL text, truncated for safety. Parameter values are never included. */
    statement?: string;
    [key: string]: any;
  };
  error?: any;
  /** Captured from the active OTEL span (typically the HTTP request span). */
  parentSpanId?: string;
  traceId?: string;
}

const MAX_STATEMENT_LENGTH = 500;

/**
 * Custom PostgreSQL instrumentation. Mirrors the Mongo/Redis pattern:
 *   - patch already-loaded `pg` modules in `require.cache`
 *   - install a `Module.prototype.require` hook so future loads are patched too
 *   - on each query, capture the active OTEL span context so Postgres ops nest
 *     under the parent HTTP span when one is active
 *
 * Patches `Client.prototype.query`, which also covers `Pool.query` (the pool
 * checks out a Client and delegates to its `query`). All three call shapes are
 * handled: callback style, promise style, and Submittable (pg-cursor /
 * QueryStream, which return an event emitter instead of a promise).
 */
export class PostgresInstrumentation implements Instrumentation {
  name = "postgres";

  private requireHookInstalled = false;
  private onSpanCallback: (span: PostgresSdkSpan) => void;

  constructor(onSpan: (span: PostgresSdkSpan) => void) {
    this.onSpanCallback = onSpan;
  }

  /** Replace the callback after construction (used by index.ts via the manager). */
  setOnSpan(callback: (span: PostgresSdkSpan) => void): void {
    this.onSpanCallback = callback;
  }

  enable(): void {
    this.patchExistingDrivers();
    this.installRequireHook();
  }

  /* ------------------------------------------------------------------ */
  /* Patch already-loaded drivers                                       */
  /* ------------------------------------------------------------------ */

  private patchExistingDrivers(): void {
    try {
      const cached = require.cache;
      for (const key of Object.keys(cached)) {
        const exports = cached[key]?.exports;
        if (!exports) continue;

        if (key.includes("node_modules/pg/")) {
          this.patchPg(exports);
        }
      }
    } catch {
      // ignore
    }
  }

  /* ------------------------------------------------------------------ */
  /* Require hook                                                       */
  /* ------------------------------------------------------------------ */

  private installRequireHook(): void {
    if (this.requireHookInstalled) return;
    this.requireHookInstalled = true;

    const originalRequire = Module.prototype.require;
    const self = this;

    Module.prototype.require = function (id: string) {
      const exports = originalRequire.apply(this, arguments as any);

      if (id === "pg") {
        self.patchPg(exports);
      }

      return exports;
    };
  }

  /* ------------------------------------------------------------------ */
  /* pg                                                                 */
  /* ------------------------------------------------------------------ */

  private patchPg(mod: any): void {
    // `pg` exports { Client, Pool, ... }; ESM interop may nest under default.
    const pg = mod?.default || mod;
    const Client = pg?.Client;
    if (!Client?.prototype?.query) return;
    if ((Client as any).__polartrace_patched) return;
    (Client as any).__polartrace_patched = true;

    const self = this;
    const originalQuery = Client.prototype.query;

    Client.prototype.query = function (config: any, values?: any, callback?: any) {
      const text = extractQueryText(config);

      // Submittables (pg-cursor, QueryStream) manage their own lifecycle; the
      // span would have no reliable end. Pass them through untouched.
      if (config && typeof config.submit === "function") {
        return originalQuery.call(this, config, values, callback);
      }

      const span = self.beginSpan(text);

      // Normalize the callback position: query(text, cb) / query(text, values, cb).
      let cb = callback;
      let vals = values;
      if (typeof values === "function") {
        cb = values;
        vals = undefined;
      }

      if (typeof cb === "function") {
        const wrappedCb = function (err: any, res: any) {
          self.end(span, err);
          return cb(err, res);
        };
        return originalQuery.call(this, config, vals, wrappedCb);
      }

      const result = originalQuery.call(this, config, vals);
      if (result && typeof result.then === "function") {
        result.then(
          () => self.end(span),
          (err: any) => self.end(span, err),
        );
      } else {
        // Unknown return shape - close the span so it is never leaked.
        self.end(span);
      }
      return result;
    };
  }

  /* ------------------------------------------------------------------ */
  /* Span helpers                                                        */
  /* ------------------------------------------------------------------ */

  private beginSpan(text: string | undefined): PostgresSdkSpan {
    const activeSpan = trace.getActiveSpan();
    let parentSpanId: string | undefined;
    let traceId: string | undefined;

    if (activeSpan) {
      const spanContext = activeSpan.spanContext();
      traceId = spanContext.traceId;
      parentSpanId = spanContext.spanId;
    }

    const operation = extractOperation(text);

    return {
      name: `postgres.${operation ? operation.toLowerCase() : "query"}`,
      startTime: Date.now(),
      attributes: {
        operation,
        table: extractTable(text),
        statement: truncateStatement(text),
      },
      parentSpanId,
      traceId,
    };
  }

  private end(span: PostgresSdkSpan, error?: any): void {
    const end = Date.now();
    span.endTime = end;
    span.durationMs = end - span.startTime;
    if (error) span.error = error;

    this.onSpanCallback(span);
  }
}

/** The SQL text from any of pg's query() call shapes. */
function extractQueryText(config: any): string | undefined {
  if (typeof config === "string") return config;
  if (config && typeof config.text === "string") return config.text;
  return undefined;
}

/** First SQL keyword, uppercased: "SELECT", "INSERT", "BEGIN", ... */
function extractOperation(text: string | undefined): string | undefined {
  if (!text) return undefined;
  const match = text.match(/^\s*([a-zA-Z]+)/);
  return match ? match[1].toUpperCase() : undefined;
}

/** Best-effort primary table: the identifier after FROM / INTO / UPDATE / JOIN. */
function extractTable(text: string | undefined): string | undefined {
  if (!text) return undefined;
  const match = text.match(/\b(?:from|into|update|join)\s+"?([a-zA-Z0-9_.]+)"?/i);
  return match ? match[1] : undefined;
}

/** Truncate the SQL text. Parameterized values are never part of it, but raw
 * statements can be arbitrarily large. */
function truncateStatement(text: string | undefined): string | undefined {
  if (!text) return undefined;
  return text.length > MAX_STATEMENT_LENGTH
    ? text.slice(0, MAX_STATEMENT_LENGTH)
    : text;
}
