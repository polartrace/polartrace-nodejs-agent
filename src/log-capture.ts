import os from "os";
import { format } from "util";
import { trace as otelTrace } from "@opentelemetry/api";

import { onModuleLoad } from "./module-hook";

/**
 * Application log capture: console output, winston loggers and process
 * crashes, from anywhere in the process - not only inside a request.
 *
 * A line written while a request is in flight belongs to that request and
 * ships inside its request log (`consoleLogs`), exactly as before. Every other
 * line - startup, a cluster primary, timers, queue consumers, work that
 * continues after the response, a crash - is a standalone line the agent
 * ships on its own stream.
 *
 * Which request a line belongs to comes from the active OpenTelemetry trace,
 * not from a store of our own: the HTTP instrumentation binds each request's
 * context to its req/res emitters, so it survives body parsing and every
 * async hop the route takes, where a fresh AsyncLocalStorage would not.
 */

/** Where a line came from. */
export type LogSource = "console" | "winston" | "process";

/** One captured line, as it ships inside a request log's `consoleLogs`. */
export interface CapturedLine {
  timestamp: string;
  /** The emitter's own level name (`log`, `http`, `warning`...); the server normalises it. */
  level: string;
  /** The line exactly as the application printed it, as a single string. */
  args: string[];
  source: LogSource;
  hostname: string;
  pid: number;
  /** Structured metadata the logger carried (winston meta, error stack...). */
  attributes?: Record<string, unknown>;
}

/** A line that belongs to no in-flight request, shipped on its own stream. */
export interface StandaloneLine {
  timestamp: string;
  level: string;
  message: string;
  source: LogSource;
  hostname: string;
  pid: number;
  attributes?: Record<string, unknown>;
  /** Set when the line ran inside a trace whose request had already finished. */
  traceId?: string;
}

interface OpenRequest {
  lines: CapturedLine[];
  openedAt: number;
}

export interface LogCaptureOptions {
  /** Receives every line that belongs to no in-flight request. */
  onStandaloneLine: (line: StandaloneLine) => void;
  /** Applied to structured attributes before they leave the process. */
  redact: (value: any) => any;
}

/** A single line longer than this is cut, so one huge dump cannot stall a batch. */
const MAX_MESSAGE_CHARS = 32_000;
/** Lines kept per request; anything past it still ships, as standalone lines. */
const MAX_LINES_PER_REQUEST = 1_000;
/** A request open this long has lost its finish event; stop routing lines to it. */
const STALE_REQUEST_MS = 10 * 60 * 1000;
/** How deep structured attributes are copied before the rest is summarised. */
const MAX_ATTRIBUTE_DEPTH = 5;

// Colour codes winston's colorize() format leaves in level and message.
// eslint-disable-next-line no-control-regex
const ANSI_PATTERN = /\u001b\[[0-9;]*m/g;

const CONSOLE_METHODS = ["log", "info", "warn", "error", "debug", "trace"] as const;

/**
 * Keys on a winston info object that are not the caller's metadata: winston's
 * own, and the trace_id / span_id / trace_flags OpenTelemetry's winston
 * instrumentation injects - the line already carries its trace.
 */
const WINSTON_RESERVED_KEYS = new Set([
  "level",
  "message",
  "timestamp",
  "stack",
  "trace_id",
  "span_id",
  "trace_flags",
]);

const truncate = (text: string): string =>
  text.length > MAX_MESSAGE_CHARS
    ? `${text.slice(0, MAX_MESSAGE_CHARS)}… [truncated ${text.length - MAX_MESSAGE_CHARS} chars]`
    : text;

/**
 * A JSON-safe copy of `value`: cycles, functions, buffers and errors become
 * plain values, and nesting is bounded. Metadata objects routinely hold
 * sockets or requests - shipping them raw would either throw on serialisation
 * (and wedge the whole batch) or send megabytes.
 */
export function toJsonSafe(value: unknown, depth = 0, seen = new WeakSet<object>()): unknown {
  if (value === null || value === undefined) return value ?? null;
  if (typeof value === "string") return truncate(value);
  if (typeof value === "number") return Number.isFinite(value) ? value : String(value);
  if (typeof value === "boolean") return value;
  if (typeof value === "bigint" || typeof value === "symbol") return value.toString();
  if (typeof value === "function") return `[Function: ${value.name || "anonymous"}]`;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? "Invalid Date" : value.toISOString();
  if (value instanceof Error) {
    return { name: value.name, message: value.message, ...(value.stack ? { stack: value.stack } : {}) };
  }
  if (Buffer.isBuffer(value)) return `[Buffer ${value.length} bytes]`;
  if (typeof value !== "object") return String(value);

  if (seen.has(value)) return "[Circular]";
  if (depth >= MAX_ATTRIBUTE_DEPTH) return Array.isArray(value) ? "[Array]" : "[Object]";
  seen.add(value);

  let result: unknown;
  if (Array.isArray(value)) {
    result = value.slice(0, 100).map((item) => toJsonSafe(item, depth + 1, seen));
  } else {
    const copy: Record<string, unknown> = {};
    for (const key of Object.keys(value).slice(0, 200)) {
      copy[key] = toJsonSafe((value as any)[key], depth + 1, seen);
    }
    result = copy;
  }
  seen.delete(value);
  return result;
}

export class LogCapture {
  private readonly hostname = os.hostname();
  private readonly pid = process.pid;
  private readonly openRequests = new Map<string, OpenRequest>();
  /** Non-zero while a winston Console transport is printing - see installWinston. */
  private suppressConsole = 0;
  /** Guards against a line recorded while recording another (a getter that logs...). */
  private recording = false;

  constructor(private readonly options: LogCaptureOptions) {}

  /**
   * Start collecting the lines of the request whose trace is `traceId`. The
   * returned array fills until `closeRequest`; lines after that are standalone.
   */
  openRequest(traceId: string | undefined): CapturedLine[] | undefined {
    if (!traceId) return undefined;
    const request: OpenRequest = { lines: [], openedAt: Date.now() };
    this.openRequests.set(traceId, request);
    return request.lines;
  }

  closeRequest(traceId: string | undefined, lines: CapturedLine[] | undefined): void {
    if (!traceId || !lines) return;
    // Only the request that opened this entry may close it: a second request
    // in the same distributed trace may have replaced it since.
    if (this.openRequests.get(traceId)?.lines === lines) {
      this.openRequests.delete(traceId);
    }
  }

  /** Drop entries whose request never finished (aborted, hijacked socket...). */
  sweepStaleRequests(now = Date.now()): void {
    for (const [traceId, request] of this.openRequests) {
      if (now - request.openedAt > STALE_REQUEST_MS) this.openRequests.delete(traceId);
    }
  }

  /** Record one line from any source. Never throws. */
  record(
    level: string,
    message: string,
    source: LogSource,
    attributes?: Record<string, unknown>,
  ): void {
    if (this.recording) return;
    this.recording = true;
    try {
      const timestamp = new Date().toISOString();
      const text = truncate(message);
      const safeAttributes =
        attributes && Object.keys(attributes).length > 0
          ? this.options.redact(toJsonSafe(attributes))
          : undefined;

      const traceId = activeTraceId();
      const request = traceId ? this.openRequests.get(traceId) : undefined;
      if (request && request.lines.length < MAX_LINES_PER_REQUEST) {
        request.lines.push({
          timestamp,
          level,
          args: [text],
          source,
          hostname: this.hostname,
          pid: this.pid,
          ...(safeAttributes ? { attributes: safeAttributes } : {}),
        });
        return;
      }

      this.options.onStandaloneLine({
        timestamp,
        level,
        message: text,
        source,
        hostname: this.hostname,
        pid: this.pid,
        ...(safeAttributes ? { attributes: safeAttributes } : {}),
        ...(traceId ? { traceId } : {}),
      });
    } catch {
      // Capture is best effort and must never break the application's logging.
    } finally {
      this.recording = false;
    }
  }

  /**
   * Wrap the console once, for the life of the process. The original method
   * always runs, with the caller's own arguments, so terminal output is
   * byte-identical to an uninstrumented process.
   */
  installConsole(): void {
    const consoleAny = console as any;
    if (consoleAny.__POLARTRACE_CONSOLE_PATCHED__) return;
    consoleAny.__POLARTRACE_CONSOLE_PATCHED__ = true;

    const originalError = consoleAny.error;
    const capture = this;

    for (const method of CONSOLE_METHODS) {
      const original = consoleAny[method];
      if (typeof original !== "function") continue;

      const patched = function (this: unknown, ...args: unknown[]) {
        if (method === "trace" && typeof originalError === "function") {
          // Node's console.trace is error("Trace: <message>\n<stack>"). Build
          // that here, with the stack starting at the caller, so the wrapper
          // never shows up as a frame in the application's output.
          const holder: { stack?: string } = {};
          Error.captureStackTrace(holder, patched);
          const frames = holder.stack?.split("\n").slice(1).join("\n");
          const text = `Trace: ${format(...args)}${frames ? `\n${frames}` : ""}`;
          if (capture.suppressConsole === 0) capture.record(method, text, "console");
          return capture.withConsoleSuppressed(() => originalError.call(this, text));
        }

        if (capture.suppressConsole === 0) {
          capture.record(method, format(...args), "console");
        }
        // Stand down while the original runs: a method that prints through
        // another console method must not be recorded twice.
        return capture.withConsoleSuppressed(() => original.apply(this, args));
      };
      consoleAny[method] = patched;
    }
  }

  private withConsoleSuppressed<T>(fn: () => T): T {
    this.suppressConsole++;
    try {
      return fn();
    } finally {
      this.suppressConsole--;
    }
  }

  /**
   * Capture every winston logger. Winston's Console transport writes straight
   * to the process streams, so console capture never sees it; instead a
   * Polartrace transport is added to each logger as it is configured. It sees
   * every line the logger lets through - whatever the logger's own transports
   * are - with its real level, metadata and error stack.
   */
  installWinston(): void {
    onModuleLoad("winston", (winston: any) => {
      try {
        this.patchWinston(winston);
      } catch {
        // An unexpected winston shape leaves the app's logging untouched.
      }
    });
  }

  private patchWinston(winston: any): void {
    const LoggerClass = winston?.Logger;
    const TransportBase = winston?.Transport;
    if (
      typeof LoggerClass?.prototype?.configure !== "function" ||
      typeof TransportBase !== "function" ||
      LoggerClass.__POLARTRACE_PATCHED__
    ) {
      return;
    }
    LoggerClass.__POLARTRACE_PATCHED__ = true;

    const capture = this;
    const LEVEL = Symbol.for("level");

    class PolartraceTransport extends TransportBase {
      constructor() {
        super({});
        (this as any).__POLARTRACE_TRANSPORT__ = true;
      }

      log(info: any, callback: () => void): void {
        try {
          capture.recordWinston(info, LEVEL);
        } finally {
          callback();
        }
      }
    }

    const originalConfigure = LoggerClass.prototype.configure;
    LoggerClass.prototype.configure = function (this: any, ...args: any[]) {
      const result = originalConfigure.apply(this, args);
      try {
        // configure() replaces the transport list, so re-add ours every time.
        const hasOurs = (this.transports || []).some((t: any) => t?.__POLARTRACE_TRANSPORT__);
        if (!hasOurs) this.add(new PolartraceTransport());
      } catch {
        // A logger we cannot extend keeps working exactly as configured.
      }
      return result;
    };

    // A Console transport created with `forceConsole` prints through
    // console.log - the line is already recorded by our transport, so console
    // capture stands down while it prints instead of recording it twice.
    const ConsoleTransport = winston?.transports?.Console;
    if (typeof ConsoleTransport?.prototype?.log === "function") {
      const originalLog = ConsoleTransport.prototype.log;
      ConsoleTransport.prototype.log = function (this: any, ...args: any[]) {
        return capture.withConsoleSuppressed(() => originalLog.apply(this, args));
      };
    }
  }

  private recordWinston(info: any, LEVEL: symbol): void {
    if (!info || typeof info !== "object") return;

    const rawLevel = String(info[LEVEL] ?? info.level ?? "info");
    const level = rawLevel.replace(ANSI_PATTERN, "");

    const rawMessage = info.message;
    const message =
      typeof rawMessage === "string"
        ? rawMessage.replace(ANSI_PATTERN, "")
        : rawMessage === undefined
          ? ""
          : format(rawMessage);

    const attributes: Record<string, unknown> = {};
    for (const key of Object.keys(info)) {
      if (!WINSTON_RESERVED_KEYS.has(key)) attributes[key] = info[key];
    }
    // `logger.error("msg", err)` and the errors() format both leave the stack
    // here - even when the app's own output format prints only the message.
    if (typeof info.stack === "string") attributes.stack = info.stack;

    this.record(level, message, "winston", attributes);
  }

  /**
   * Record crashes without changing how the process crashes. The monitor
   * event observes an uncaught exception (and an unhandled rejection that
   * Node turns into one) without counting as a handler, so Node still prints
   * the error and exits exactly as it would without the agent.
   */
  installProcessHooks(): void {
    const globalObj = globalThis as any;
    if (globalObj.__POLARTRACE_CRASH_HOOK__) return;
    globalObj.__POLARTRACE_CRASH_HOOK__ = true;

    process.on("uncaughtExceptionMonitor", (error: unknown, origin: string) => {
      const what = origin === "unhandledRejection" ? "Unhandled promise rejection" : "Uncaught exception";
      this.record("error", `${what}: ${format(error)}`, "process", {
        origin,
        ...(error instanceof Error && error.stack ? { stack: error.stack } : {}),
      });
    });
  }
}

/** The active OpenTelemetry trace id, or undefined outside any trace. */
function activeTraceId(): string | undefined {
  try {
    const id = otelTrace.getActiveSpan()?.spanContext().traceId;
    // OTel's "invalid" sentinel is 32 zeros - treat as no trace.
    if (!id || /^0+$/.test(id)) return undefined;
    return id;
  } catch {
    return undefined;
  }
}
