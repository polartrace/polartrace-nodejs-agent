import { onModuleLoad } from "../module-hook";
import { Instrumentation } from "./instrumentation";
import { trace } from "@opentelemetry/api";

export interface RedisSdkSpan {
  name: string;
  startTime: number;
  endTime?: number;
  durationMs?: number;
  attributes: {
    /** Library that produced the span: "ioredis" | "redis" */
    library?: string;
    /** Redis command name, e.g. "GET", "SET", "HSET". */
    command?: string;
    /** First non-command arg (typically the key). Truncated for safety. */
    key?: string;
    [key: string]: any;
  };
  error?: any;
  /** Captured from the active OTEL span (typically the HTTP request span). */
  parentSpanId?: string;
  traceId?: string;
}

/**
 * Custom Redis instrumentation. Mirrors the Mongo instrumentation pattern:
 *   - patch already-loaded driver modules in `require.cache`
 *   - install a `Module.prototype.require` hook so future loads are patched too
 *   - on each command, capture the active OTEL span context so Redis ops nest
 *     under the parent HTTP span when one is active
 *
 * Supports both `ioredis` and `redis` (node-redis v4+). They are the two
 * libraries Polartrace customers actually use; both are detected at runtime
 * and patched independently, so an app using both still emits a single span
 * per command from a single source of truth.
 */
export class RedisInstrumentation implements Instrumentation {
  name = "redis";

  private requireHookInstalled = false;
  private onSpanCallback: (span: RedisSdkSpan) => void;

  constructor(onSpan: (span: RedisSdkSpan) => void) {
    this.onSpanCallback = onSpan;
  }

  /** Replace the callback after construction (used by index.ts via the manager). */
  setOnSpan(callback: (span: RedisSdkSpan) => void): void {
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

        if (key.includes("node_modules/ioredis")) {
          this.patchIoredis(exports);
        } else if (
          key.includes("node_modules/redis/") ||
          key.endsWith("node_modules/redis/dist/index.js")
        ) {
          this.patchNodeRedis(exports);
        }
      }
    } catch {
      // ignore
    }
  }

  /* ------------------------------------------------------------------ */
  /* Require hook                                                       */
  /* ------------------------------------------------------------------ */

  /**
   * Hook the driver's module load. Goes through the shared `onModuleLoad`
   * helper rather than `Module.prototype.require` directly, so the driver is
   * patched for ESM entry points too - `import` never reaches `require`.
   */
  private installRequireHook(): void {
    if (this.requireHookInstalled) return;
    this.requireHookInstalled = true;

    const self = this;

    onModuleLoad("ioredis", (exports) => self.patchIoredis(exports));
    onModuleLoad("redis", (exports) => self.patchNodeRedis(exports));
  }

  /* ------------------------------------------------------------------ */
  /* ioredis                                                            */
  /* ------------------------------------------------------------------ */

  private patchIoredis(mod: any): void {
    // ioredis exports the class as `module.exports` (CJS) or `default` (ESM interop)
    const Redis = mod?.default || mod;
    if (!Redis?.prototype?.sendCommand) return;
    if ((Redis as any).__polartrace_patched) return;
    (Redis as any).__polartrace_patched = true;

    const self = this;
    const originalSendCommand = Redis.prototype.sendCommand;

    Redis.prototype.sendCommand = function (command: any, stream?: any) {
      const span = self.beginSpan("ioredis", command?.name, command?.args);
      const result = originalSendCommand.call(this, command, stream);

      // ioredis attaches `.promise` to every Command; settle the span on it.
      const promise = command?.promise;
      if (promise && typeof promise.then === "function") {
        promise.then(
          () => self.end(span),
          (err: any) => self.end(span, err),
        );
      } else {
        // Fallback - we couldn't attach to a result, close span immediately.
        self.end(span);
      }

      return result;
    };
  }

  /* ------------------------------------------------------------------ */
  /* node-redis (v4+)                                                   */
  /* ------------------------------------------------------------------ */

  private patchNodeRedis(mod: any): void {
    if (!mod || typeof mod.createClient !== "function") return;
    if ((mod as any).__polartrace_patched) return;
    (mod as any).__polartrace_patched = true;

    const self = this;
    const originalCreateClient = mod.createClient;

    mod.createClient = function (...args: any[]) {
      const client = originalCreateClient.apply(this, args);
      self.patchNodeRedisClient(client);
      return client;
    };

    // node-redis also exposes createCluster which produces a different client shape
    if (typeof mod.createCluster === "function") {
      const originalCreateCluster = mod.createCluster;
      mod.createCluster = function (...args: any[]) {
        const cluster = originalCreateCluster.apply(this, args);
        self.patchNodeRedisClient(cluster);
        return cluster;
      };
    }
  }

  /**
   * node-redis v4 generates its command methods (`set`, `hGetAll`, ...) onto the
   * client prototype, and each one calls an executor captured at class-definition
   * time which reaches the connection through a PRIVATE `#sendCommand`. Wrapping
   * the public `client.sendCommand` - instance or prototype - therefore never
   * intercepts a single command: nothing calls it. The generated methods
   * themselves are the only reachable seam, so they are what we wrap.
   *
   * Generated commands are identified by their body delegating to `executor`.
   * If a future release changes that shape we simply stop producing spans,
   * rather than wrapping a lifecycle method (`connect`, `quit`, `multi`, ...)
   * and breaking the client.
   */
  private patchNodeRedisClient(client: any): void {
    if (!client) return;

    let proto = Object.getPrototypeOf(client);
    while (proto && !Object.prototype.hasOwnProperty.call(proto, "sendCommand")) {
      proto = Object.getPrototypeOf(proto);
    }
    if (!proto || proto.__polartrace_patched) return;
    proto.__polartrace_patched = true;

    const self = this;

    for (const name of Object.getOwnPropertyNames(proto)) {
      let original: any;
      try {
        original = proto[name];
      } catch {
        continue;
      }
      if (typeof original !== "function") continue;

      let isGeneratedCommand = false;
      try {
        isGeneratedCommand = /executor\.call\(/.test(Function.prototype.toString.call(original));
      } catch {
        continue;
      }
      if (!isGeneratedCommand) continue;

      proto[name] = function (this: any, ...args: any[]) {
        const span = self.beginSpan("redis", name.toLowerCase(), args);
        let result: any;
        try {
          result = original.apply(this, args);
        } catch (err) {
          self.end(span, err);
          throw err;
        }
        if (result && typeof result.then === "function") {
          return result.then(
            (value: any) => {
              self.end(span);
              return value;
            },
            (err: any) => {
              self.end(span, err);
              throw err;
            },
          );
        }
        self.end(span);
        return result;
      };
    }
  }

  /* ------------------------------------------------------------------ */
  /* Span helpers                                                        */
  /* ------------------------------------------------------------------ */

  private beginSpan(
    library: string,
    command: string | undefined,
    args: any,
  ): RedisSdkSpan {
    const activeSpan = trace.getActiveSpan();
    let parentSpanId: string | undefined;
    let traceId: string | undefined;

    if (activeSpan) {
      const spanContext = activeSpan.spanContext();
      traceId = spanContext.traceId;
      parentSpanId = spanContext.spanId;
    }

    return {
      name: `redis.${command || "unknown"}`,
      startTime: Date.now(),
      attributes: {
        library,
        command,
        key: extractKey(args),
      },
      parentSpanId,
      traceId,
    };
  }

  private end(span: RedisSdkSpan, error?: any): void {
    const end = Date.now();
    span.endTime = end;
    span.durationMs = end - span.startTime;
    if (error) span.error = error;

    this.onSpanCallback(span);
  }
}

/**
 * Best-effort extraction of the first non-command argument (typically the key).
 * We truncate to keep span payloads small.
 */
function extractKey(args: any): string | undefined {
  if (!Array.isArray(args) || args.length === 0) return undefined;
  const first = args[0];
  if (first === null || first === undefined) return undefined;
  const str = typeof first === "string" ? first : String(first);
  return str.length > 128 ? str.slice(0, 128) : str;
}
