import * as fs from "fs";

export type DiagnosticLevel = "error" | "warn" | "info" | "debug";

const PREFIX = "[polartrace]";

export interface DiagnosticsOptions {
  /** Emit `info` / `debug` to stderr as well (POLARTRACE_ENABLE_CONSOLE_LOG). */
  verbose?: boolean;
  /** Append-only diagnostics file (POLARTRACE_LOG_FILE). No file when unset. */
  filePath?: string;
}

/**
 * Agent diagnostics.
 *
 * Two independent sinks:
 *
 *  - stderr, which always carries `error` / `warn` so a misconfigured agent is
 *    never silently dropping telemetry, and carries `info` / `debug` only in
 *    verbose mode. stdout is deliberately untouched so the agent can never
 *    corrupt an application that writes structured logs there.
 *  - an append-only file, opened only when the application asks for one. That
 *    sink receives every level regardless of verbosity: a file is requested in
 *    order to diagnose a problem, so it gets the full picture.
 *
 * Writes go through `process.stderr` rather than `console.*` on purpose: the
 * agent patches the console to capture application output, and routing its own
 * diagnostics through that patch would ship them back to the collector as if
 * the application had logged them.
 */
class Diagnostics {
  private verbose = false;
  private stream?: fs.WriteStream;
  private readonly announced = new Set<string>();

  configure(options: DiagnosticsOptions): void {
    if (options.verbose !== undefined) {
      this.verbose = options.verbose;
    }

    const filePath = options.filePath?.trim();
    if (!filePath || this.stream) {
      return;
    }

    try {
      this.stream = fs.createWriteStream(filePath, { flags: "a" });
      // A disk that fills up, or a path that is revoked, must not raise an
      // unhandled 'error' event inside the host application.
      this.stream.on("error", () => {
        this.stream = undefined;
      });
    } catch (error) {
      process.stderr.write(
        `${PREFIX} could not open log file ${filePath}: ${(error as Error).message}\n`,
      );
    }
  }

  error(message: string): void {
    this.emit("error", message);
  }

  warn(message: string): void {
    this.emit("warn", message);
  }

  info(message: string): void {
    this.emit("info", message);
  }

  debug(message: string): void {
    this.emit("debug", message);
  }

  /**
   * Report a recurring condition (a collector that keeps rejecting batches,
   * say) on stderr once per `key`, while still recording every occurrence in
   * the diagnostics file. Without this a permanently rejected batch would
   * print on every flush tick for the lifetime of the process.
   */
  warnOnce(key: string, message: string): void {
    const alreadyAnnounced = this.announced.has(key);
    this.announced.add(key);
    this.emit("warn", message, { toStderr: !alreadyAnnounced });
  }

  private emit(
    level: DiagnosticLevel,
    message: string,
    options?: { toStderr?: boolean },
  ): void {
    if (this.stream) {
      try {
        this.stream.write(
          `${new Date().toISOString()} ${level.toUpperCase().padEnd(5)} ${message}\n`,
        );
      } catch {
        // Diagnostics must never throw into the host application.
      }
    }

    const isProblem = level === "error" || level === "warn";
    const toStderr = options?.toStderr ?? true;
    if (toStderr && (isProblem || this.verbose)) {
      process.stderr.write(`${PREFIX} ${message}\n`);
    }
  }

  close(): void {
    if (!this.stream) {
      return;
    }
    try {
      this.stream.end();
    } catch {
      // Nothing useful to do if the stream is already gone.
    }
    this.stream = undefined;
  }
}

export const diagnostics = new Diagnostics();
