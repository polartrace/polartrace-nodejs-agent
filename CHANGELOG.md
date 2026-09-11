# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-09-12

The first release of the 2.x line - a ground-up hardening of the 1.x agent.
Everything below ships together as 2.0.0.

### Breaking

- The agent no longer creates `polartrace_agent.log` in the working directory.
  Diagnostics are off by default; set `POLARTRACE_LOG_FILE=<path>` to get an
  append-only diagnostics file at a location you control
- Conditions that cause silent data loss (unrecognised license key, unreachable
  collector, batches the collector rejects) are now reported on **stderr** as a
  single `[polartrace] ...` line, once per condition. Nothing is ever written to
  stdout
- A misconfigured agent under `-r polartrace` now disables itself and reports
  why, instead of throwing during preload and preventing the host application
  from starting
- `express` is no longer a required peer dependency. `express`, `fastify`, `koa`
  and `mongoose` are all optional peers, so a Fastify- or Koa-only application no
  longer pulls in Express
- `mongodb`, `mongoose` and `axios` are no longer runtime dependencies; the agent
  patches the host application's own drivers and uses native `fetch`

### Added

- Fastify (4.x, 5.x) and Koa (2.x, 3.x) request-log capture, attached
  automatically at instance creation
- PostgreSQL span collection via the `pg` driver, with `db.system`,
  `db.operation`, `db.sql.table` and `db.statement` attributes
  (opt-out: `POLARTRACE_DISABLE_POSTGRES_SPANS`)
- Redis span collection for `ioredis` and node-redis v4+
  (opt-out: `POLARTRACE_DISABLE_REDIS_SPANS`)
- Host metrics sampling: process CPU/memory, event-loop lag percentiles, host
  load and memory, container cgroup limits
  (opt-out: `POLARTRACE_DISABLE_HOST_METRICS`)
- TypeScript entry points run under `-r polartrace` by picking up `tsx` or
  `ts-node` from the host application
- Queued telemetry is flushed on `SIGTERM`, `SIGINT` and `beforeExit`, deferring
  to the application's own signal handlers when it has them
- `POLARTRACE_ENDPOINT` to override the collector URL
- `POLARTRACE_LOG_FILE` and the `logFile` config option

### Changed

- OpenTelemetry upgraded to the 2.x core line. HTTP spans carry both the stable
  semantic conventions (`http.request.method`, `url.path`, ...) and their legacy
  equivalents (`http.method`, `http.target`, ...), so the collector's wire format
  is unchanged by the upgrade
- `@types/express` moved to devDependencies; it is no longer installed by
  consumers that do not use Express
- Batches are retried only on 429/5xx/network errors, and dropped with a warning
  on other 4xx responses (401/402/400/413) that can never succeed on retry
- Retained failures back off exponentially (10s doubling to a 5 minute ceiling)
  and honour the collector's `Retry-After` header on 429
- Log and trace queues are bounded at 5000 items (drop-oldest), matching host
  metrics, so a long collector outage cannot grow memory without limit
- Flushes ship at most 500 items per tick instead of the entire queue
- Flush timers are `unref()`ed, so the agent never keeps the host process alive
- Spans now report the agent's real version as `service.version` instead of a
  hardcoded `1.0.0`
- The published package no longer contains internal implementation comments

### Fixed

- NestJS route exceptions now carry their message and stack in the request
  log on both Nest platforms (Express and Fastify). Nest's exception layer
  catches every route error and responds itself, so nothing ever reached the
  underlying framework's error paths; the agent now records the exception
  from `BaseExceptionFilter.catch` before Nest responds. 4xx `HttpException`s
  (`NotFoundException`, validation 400s...) are expected control flow and are
  deliberately not recorded as errors
- Express thrown/`next(err)` route errors now carry their message and stack in
  the request log. They travel Express's error-middleware chain (never the
  req/res `error` events the agent listened on), so a 500 was recorded with an
  empty error. The agent now appends a pass-through error middleware at
  `listen()` time, after the user's stack is complete; Koa and Fastify already
  captured these via their own hooks
- Request headers are redacted before transmission. `Authorization`, `Cookie` and
  `X-API-Key` were previously shipped to the collector in clear text on every
  request, despite the documented redaction policy. `cookie` and `api-key` were
  added to the sensitive-field list, which now covers bodies and headers alike
- Express now captures the request body. It was read at middleware entry, before
  `express.json()` had parsed anything, so the `body` field was always absent -
  Fastify and Koa were unaffected
- node-redis (v4+) commands now produce spans. The patch targeted the public
  `client.sendCommand`, which node-redis' generated commands never call - they
  reach the connection through a private method - so the instrumentation was
  silently inert. The generated command methods are now wrapped directly
- Fastify 5 and Koa 3 spans carry `http.route` again. The pinned OpenTelemetry
  bundle predated both, so every endpoint collapsed into a bare `GET`/`POST` span
  with no route. The OpenTelemetry stack was upgraded and Fastify's
  instrumentation, no longer part of the auto-instrumentations bundle, is
  registered explicitly
- ESM entry points now produce request logs and database spans. Auto-attach was
  installed on `Module.prototype.require`, which `import` never reaches, so an
  ESM application silently lost all request logging. Hooks now go through
  `Module._load`, which both module systems use
- Inline SQL string literals in `db.statement` are replaced with `'?'`
- Agent ingest paths are excluded from tracing by exact path. The previous bare
  `/log` / `/logs` / `/traces` fragments matched any application route
  containing them and erased those routes from tracing

---

Releases before 2.0.0 (the 1.x line) predate the public repository and are
intentionally not documented here.
