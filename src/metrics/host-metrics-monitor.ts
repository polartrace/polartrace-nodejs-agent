import * as os from "os";
import * as fs from "fs";
import { monitorEventLoopDelay, IntervalHistogram } from "perf_hooks";


export interface HostMetricSample {
  /** Wall-clock time in ms since epoch when the sample was taken */
  timestamp: number;

  /** Process-level metrics (the Node app the agent is loaded into) */
  process: {
    pid: number;
    /** % CPU used by THIS process across all cores during the last interval (0-100*cpuCount). */
    cpuPercent: number;
    /** Cumulative user CPU time in microseconds (`process.cpuUsage().user`). */
    cpuUserMicros: number;
    /** Cumulative system CPU time in microseconds (`process.cpuUsage().system`). */
    cpuSystemMicros: number;
    /** Resident set size in bytes. */
    rssBytes: number;
    /** V8 heap currently used (bytes). */
    heapUsedBytes: number;
    /** V8 heap total committed (bytes). */
    heapTotalBytes: number;
    /** Memory used by C++ objects bound to JS (bytes). */
    externalBytes: number;
    /** Bytes in pre-allocated ArrayBuffer pools (bytes). */
    arrayBuffersBytes: number;
    /** Process uptime in seconds. */
    uptimeSeconds: number;
  };

  /** Event-loop responsiveness over the last interval (ms). */
  eventLoop: {
    /** Mean event-loop delay during the interval (ms). */
    meanMs: number;
    /** p50 event-loop delay (ms). */
    p50Ms: number;
    /** p95 event-loop delay (ms). */
    p95Ms: number;
    /** p99 event-loop delay (ms). */
    p99Ms: number;
    /** Max event-loop delay observed during the interval (ms). */
    maxMs: number;
  };

  /** Host / VM metrics. NOTE: inside a container these may reflect the node, not the cgroup limits. */
  host: {
    hostname: string;
    platform: string;
    cpuCount: number;
    /** 1-minute load average (host-wide). 0 on Windows. */
    loadAvg1: number;
    /** 5-minute load average. */
    loadAvg5: number;
    /** 15-minute load average. */
    loadAvg15: number;
    /** Free memory on the host (bytes). */
    freeMemBytes: number;
    /** Total memory on the host (bytes). */
    totalMemBytes: number;
    /** Host uptime in seconds. */
    uptimeSeconds: number;
    /**
     * Container-aware values (cgroup v2). Populated only when the agent is
     * running inside a Linux container; otherwise omitted. These reflect the
     * limits/usage the orchestrator actually enforces.
     */
    container?: {
      /** Memory limit from cgroup (bytes). May be `null` if unlimited. */
      memoryLimitBytes: number | null;
      /** Memory currently used by the cgroup (bytes). */
      memoryUsageBytes: number | null;
      /** CPU quota / period as a fractional core count, e.g. `0.5` for 500m. `null` if unlimited. */
      cpuLimitCores: number | null;
    };
  };
}

export interface HostMetricsMonitorConfig {
  serviceName: string;
  /** Sampling interval in ms. Default: 10000 (10s) - matches the agent's flush cadence. */
  sampleIntervalMs?: number;
  /** Optional resolution for the event-loop histogram. Default: 20ms. */
  eventLoopResolutionMs?: number;
  /** Called once per sample with the captured snapshot. */
  onSample: (sample: HostMetricSample) => void;
  /** Called when the monitor errors during sampling. */
  onError?: (err: Error) => void;
}

const NS_PER_MS = 1_000_000;

const safePercentile = (
  histogram: IntervalHistogram,
  percentile: number,
): number => {
  try {
    return histogram.percentile(percentile) / NS_PER_MS;
  } catch {
    return 0;
  }
};

/**
 * Read a cgroup file and return its trimmed text content, or `null` on error.
 */
const readCgroupFile = (path: string): string | null => {
  try {
    return fs.readFileSync(path, "utf-8").trim();
  } catch {
    return null;
  }
};

/**
 * Collect cgroup v2 container metrics if available. Returns `undefined` when
 * the agent is not running in a recognizable cgroup v2 environment.
 *
 * cgroup v2 lays out files as:
 *   /sys/fs/cgroup/memory.max     -> "max" or bytes
 *   /sys/fs/cgroup/memory.current -> bytes
 *   /sys/fs/cgroup/cpu.max        -> "<quota> <period>" or "max <period>"
 */
const readContainerMetrics = ():
  | HostMetricSample["host"]["container"]
  | undefined => {
  if (process.platform !== "linux") return undefined;

  const memMax = readCgroupFile("/sys/fs/cgroup/memory.max");
  const memCurrent = readCgroupFile("/sys/fs/cgroup/memory.current");
  const cpuMax = readCgroupFile("/sys/fs/cgroup/cpu.max");

  if (memMax === null && memCurrent === null && cpuMax === null) {
    return undefined;
  }

  const memoryLimitBytes =
    memMax === null || memMax === "max" ? null : Number.parseInt(memMax, 10);
  const memoryUsageBytes =
    memCurrent === null ? null : Number.parseInt(memCurrent, 10);

  let cpuLimitCores: number | null = null;
  if (cpuMax) {
    const [quotaStr, periodStr] = cpuMax.split(/\s+/);
    if (quotaStr && quotaStr !== "max" && periodStr) {
      const quota = Number.parseInt(quotaStr, 10);
      const period = Number.parseInt(periodStr, 10);
      if (!Number.isNaN(quota) && !Number.isNaN(period) && period > 0) {
        cpuLimitCores = quota / period;
      }
    }
  }

  return {
    memoryLimitBytes:
      memoryLimitBytes !== null && Number.isFinite(memoryLimitBytes)
        ? memoryLimitBytes
        : null,
    memoryUsageBytes:
      memoryUsageBytes !== null && Number.isFinite(memoryUsageBytes)
        ? memoryUsageBytes
        : null,
    cpuLimitCores,
  };
};

export class HostMetricsMonitor {
  private readonly config: Required<
    Omit<HostMetricsMonitorConfig, "onError">
  > & { onError?: (err: Error) => void };

  private timer?: NodeJS.Timeout;
  private histogram?: IntervalHistogram;

  private prevCpu: NodeJS.CpuUsage = { user: 0, system: 0 };
  private prevWallMs = 0;

  /** Whether `initialize()` has finished successfully. */
  public initialized = false;

  constructor(config: HostMetricsMonitorConfig) {
    this.config = {
      sampleIntervalMs: 10_000,
      eventLoopResolutionMs: 20,
      ...config,
    };
  }

  initialize(): void {
    if (this.initialized) return;

    try {
      this.histogram = monitorEventLoopDelay({
        resolution: this.config.eventLoopResolutionMs,
      });
      this.histogram.enable();

      this.prevCpu = process.cpuUsage();
      this.prevWallMs = Date.now();

      this.timer = setInterval(() => {
        try {
          this.sample();
        } catch (err: any) {
          this.config.onError?.(
            err instanceof Error ? err : new Error(String(err)),
          );
        }
      }, this.config.sampleIntervalMs);

      // Don't keep the event loop alive solely for the metrics timer.
      this.timer.unref?.();

      this.initialized = true;
    } catch (err: any) {
      this.config.onError?.(
        err instanceof Error ? err : new Error(String(err)),
      );
    }
  }

  destroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    if (this.histogram) {
      this.histogram.disable();
      this.histogram = undefined;
    }
    this.initialized = false;
  }

  private sample(): void {
    const nowMs = Date.now();
    const cpu = process.cpuUsage();
    const wallDeltaMs = Math.max(1, nowMs - this.prevWallMs);
    const userDeltaMicros = cpu.user - this.prevCpu.user;
    const systemDeltaMicros = cpu.system - this.prevCpu.system;

    // CPU% across all cores: deltaCpuMicros / (deltaWallMicros) * 100
    // We deliberately do NOT divide by core count - value above 100 indicates
    // the process is using more than one core, which is useful diagnostic info.
    const cpuPercent =
      ((userDeltaMicros + systemDeltaMicros) / (wallDeltaMs * 1_000)) * 100;

    this.prevCpu = cpu;
    this.prevWallMs = nowMs;

    const mem = process.memoryUsage();
    const histogram = this.histogram;

    const eventLoop = histogram
      ? {
          meanMs: histogram.mean / NS_PER_MS,
          p50Ms: safePercentile(histogram, 50),
          p95Ms: safePercentile(histogram, 95),
          p99Ms: safePercentile(histogram, 99),
          maxMs: histogram.max / NS_PER_MS,
        }
      : { meanMs: 0, p50Ms: 0, p95Ms: 0, p99Ms: 0, maxMs: 0 };

    histogram?.reset();

    const loadAvg = os.loadavg();
    const sample: HostMetricSample = {
      timestamp: nowMs,
      process: {
        pid: process.pid,
        cpuPercent: Number.isFinite(cpuPercent) ? cpuPercent : 0,
        cpuUserMicros: cpu.user,
        cpuSystemMicros: cpu.system,
        rssBytes: mem.rss,
        heapUsedBytes: mem.heapUsed,
        heapTotalBytes: mem.heapTotal,
        externalBytes: mem.external,
        arrayBuffersBytes: (mem as any).arrayBuffers ?? 0,
        uptimeSeconds: process.uptime(),
      },
      eventLoop,
      host: {
        hostname: os.hostname(),
        platform: process.platform,
        cpuCount: os.cpus().length,
        loadAvg1: loadAvg[0] ?? 0,
        loadAvg5: loadAvg[1] ?? 0,
        loadAvg15: loadAvg[2] ?? 0,
        freeMemBytes: os.freemem(),
        totalMemBytes: os.totalmem(),
        uptimeSeconds: os.uptime(),
        container: readContainerMetrics(),
      },
    };

    this.config.onSample(sample);
  }
}
