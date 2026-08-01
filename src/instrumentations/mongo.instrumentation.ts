import Module from "module";
import { Instrumentation } from "./instrumentation";
import { trace } from "@opentelemetry/api";

export interface MongoSdkSpan {
  name: string;
  startTime: number;
  endTime?: number;
  durationMs?: number;
  attributes: {
    db?: string;
    command?: string;
    collection?: string;
    [key: string]: any;
  };
  error?: any;
  // Parent span context - captured from active OpenTelemetry span (typically the HTTP request span)
  parentSpanId?: string;
  traceId?: string;
}

export class MongoInstrumentation implements Instrumentation {
  name = "mongodb";

  private trackedClients = new WeakSet<any>();
  private spans = new Map<number, MongoSdkSpan>();
  private requireHookInstalled = false;
  private onSpanCallback: (span: MongoSdkSpan) => void;

  constructor(
    onSpan: (span: MongoSdkSpan) => void,
    private readonly mongoose?: any, // OPTIONAL, dev-safe
  ) {
    this.onSpanCallback = onSpan;
  }

  /**
   * Update the onSpan callback (allows replacing the callback after construction)
   */
  setOnSpan(callback: (span: MongoSdkSpan) => void): void {
    this.onSpanCallback = callback;
  }

  enable(): void {
    this.patchExistingDriver();
    this.installRequireHook();

    // 🔥 CRITICAL: attach after mongoose connects
    if (this.mongoose?.connection) {
      this.mongoose.connection.once("open", () => {
        const client = this.mongoose.connection.client;
        if (client) {
          this.attach(client);
        }
      });
    }
  }

  /* ------------------------------------------------------------------ */
  /* Patch already-loaded driver                                        */
  /* ------------------------------------------------------------------ */

  private patchExistingDriver(): void {
    try {
      // If mongoose was passed, patch its driver directly
      if (this.mongoose?.mongo) {
        this.patchMongoDriver(this.mongoose.mongo);
      }

      // If mongodb already loaded anywhere, patch it
      const cached = require.cache;
      for (const key of Object.keys(cached)) {
        if (key.includes("mongodb")) {
          const exports = cached[key]?.exports;
          if (exports?.MongoClient) {
            this.patchMongoDriver(exports);
          }
        }
      }
    } catch {
      // ignore
    }
  }

  /* ------------------------------------------------------------------ */
  /* Require hook (future loads)                                        */
  /* ------------------------------------------------------------------ */

  private installRequireHook(): void {
    if (this.requireHookInstalled) return;
    this.requireHookInstalled = true;

    const originalRequire = Module.prototype.require;
    const self = this;

    Module.prototype.require = function (id: string) {
      const exports = originalRequire.apply(this, arguments as any);

      if (id === "mongodb") {
        self.patchMongoDriver(exports);
      }

      return exports;
    };
  }

  /* ------------------------------------------------------------------ */
  /* MongoDB driver patch                                                */
  /* ------------------------------------------------------------------ */

  private patchMongoDriver(mongodb: any): void {
    const MongoClient = mongodb?.MongoClient;
    if (!MongoClient) return;

    if ((MongoClient as any).__polartrace_patched) return;
    (MongoClient as any).__polartrace_patched = true;

    const self = this;
    const originalConnect = MongoClient.prototype.connect;

    MongoClient.prototype.connect = async function (...args: any[]) {
      // Ensure command monitoring
      this.options ??= {};
      // this.options.monitorCommands = true;

      const result = await originalConnect.apply(this, args);
      self.attach(this);

      return result;
    };
  }

  /* ------------------------------------------------------------------ */
  /* Client instrumentation                                             */
  /* ------------------------------------------------------------------ */

  private attach(client: any): void {
    if (this.trackedClients.has(client)) return;
    this.trackedClients.add(client);

    const emitter = client.topology || client?.s?.topology;
    if (!emitter?.on) return;

    emitter.on("commandStarted", (ev: any) => {
      // Capture the active OpenTelemetry span context to maintain parent-child relationship
      // When MongoDB operations happen during an HTTP request, the active span will be the HTTP span
      const activeSpan = trace.getActiveSpan();
      let parentSpanId: string | undefined;
      let traceId: string | undefined;

      if (activeSpan) {
        const spanContext = activeSpan.spanContext();
        traceId = spanContext.traceId;
        parentSpanId = spanContext.spanId; // The HTTP span's spanId becomes the parent
      }

      this.spans.set(ev.requestId, {
        name: `mongodb.${ev.commandName}`,
        startTime: Date.now(),
        attributes: {
          db: ev.databaseName,
          command: ev.commandName,
          collection: extractCollection(ev.command),
        },
        parentSpanId, // Will be set if there's an active HTTP span
        traceId, // Will be set if there's an active HTTP span
      });
    });

    emitter.on("commandSucceeded", (ev: any) => {
      this.end(ev.requestId);
    });

    emitter.on("commandFailed", (ev: any) => {
      this.end(ev.requestId, ev.failure);
    });
  }

  private end(id: number, error?: any): void {
    const span = this.spans.get(id);
    if (!span) return;

    const end = Date.now();
    span.endTime = end;
    span.durationMs = end - span.startTime;
    if (error) span.error = error;

    this.onSpanCallback(span);

    this.spans.delete(id);
  }
}

function extractCollection(cmd: any): string | undefined {
  const key = Object.keys(cmd || {})[0];
  return typeof cmd?.[key] === "string" ? cmd[key] : undefined;
}
