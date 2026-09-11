# polartrace

[![npm version](https://img.shields.io/npm/v/polartrace.svg)](https://www.npmjs.com/package/polartrace)
[![node](https://img.shields.io/node/v/polartrace.svg)](https://nodejs.org)
[![license](https://img.shields.io/npm/l/polartrace.svg)](LICENSE)

The Node.js agent for [PolarTrace](https://www.polartrace.com). It collects
distributed traces, request logs and host metrics from your service and ships
them to the PolarTrace collector - without a single line of application code.

Built on [OpenTelemetry](https://opentelemetry.io).

## Installation

```bash
npm install polartrace
```

Requires Node.js 18 or later.

## Quick start

Preload the agent and give it a service name and license key:

```bash
POLARTRACE_APP_NAME=checkout-service \
POLARTRACE_LICENSE_KEY=<your-license-key> \
node -r polartrace server.js
```

That's it. The agent starts before your application, instruments the frameworks
and drivers it finds, and begins reporting. If your entry point is TypeScript,
run it the same way - the agent picks up `tsx` or `ts-node` from your
dependencies automatically.

### Docker

```dockerfile
FROM node:20-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .

ENV POLARTRACE_APP_NAME=checkout-service
# Supply POLARTRACE_LICENSE_KEY at runtime, not in the image.
CMD ["node", "-r", "polartrace", "server.js"]
```

```yaml
services:
  app:
    build: .
    environment:
      POLARTRACE_APP_NAME: checkout-service
      POLARTRACE_LICENSE_KEY: ${POLARTRACE_LICENSE_KEY}
    command: node -r polartrace server.js
```

## What gets instrumented

| Area | Support | Notes |
| --- | --- | --- |
| HTTP server & client | Automatic | Inbound and outbound requests, via OpenTelemetry |
| Express | 4.x | Middleware attached automatically at app creation |
| Fastify | 4.x, 5.x | Hooks attached automatically at instance creation |
| Koa | 2.x, 3.x | Middleware prepended automatically |
| NestJS | 9.x, 10.x | Via its Express/Fastify platform; route exceptions captured from Nest's exception layer |
| MongoDB / Mongoose | mongoose ≥ 6 | Query spans with collection and operation |
| PostgreSQL | `pg` | Query spans with `db.operation`, `db.sql.table`, `db.statement` |
| Redis | `ioredis`, `redis` ≥ 4 | Command spans |
| Host metrics | Automatic | Process CPU/memory, event-loop lag, host load, cgroup limits |

Every framework and driver above is optional. The agent patches only what your
application actually loads, and installs nothing on your behalf.

## Configuration

The agent is configured entirely through environment variables.

### Required

| Variable | Description |
| --- | --- |
| `POLARTRACE_APP_NAME` | Service name. Must be unique within your organization. |
| `POLARTRACE_LICENSE_KEY` | Your PolarTrace license key. |

### Optional

| Variable | Default | Description |
| --- | --- | --- |
| `POLARTRACE_ENDPOINT` | `https://collector.polartrace.io/api/log` | Collector URL. |
| `POLARTRACE_LOG_FILE` | *(none)* | Append the agent's own diagnostics to this path. |
| `POLARTRACE_ENABLE_CONSOLE_LOG` | `false` | Mirror verbose agent diagnostics to stderr. |
| `POLARTRACE_DISABLE_HOST_METRICS` | `false` | Turn off CPU/memory/event-loop sampling. |
| `POLARTRACE_DISABLE_MONGO_SPANS` | `false` | Turn off MongoDB span collection. |
| `POLARTRACE_DISABLE_POSTGRES_SPANS` | `false` | Turn off PostgreSQL span collection. |
| `POLARTRACE_DISABLE_REDIS_SPANS` | `false` | Turn off Redis span collection. |

Boolean variables accept `1` or `true`.

## Data collection and redaction

For each request the agent records method, path, status code, duration, client
IP, user agent, headers, query parameters, body and any console output produced
while handling it, along with the trace the request belongs to.

Fields whose names contain `password`, `token`, `secret`, `apiKey`, `api-key`,
`authorization`, `creditCard`, `ssn` or `cookie` are replaced with `[REDACTED]`
before anything leaves the process - in nested objects and in **request headers**
as well as at the top level, so `Authorization`, `Cookie` and `X-API-Key` never
leave in the clear. Redaction happens client-side; the original values are never
transmitted.

SQL captured in `db.statement` has inline string literals replaced with `'?'`, so
a query that interpolates a secret does not ship it. Parameterised queries are
unaffected - the driver never hands the agent the values.

If your payloads carry sensitive data under names outside that list, turn off
body capture with the programmatic API below.

## Agent diagnostics

The agent is quiet by design. It writes nothing to stdout and creates no files.

Problems that would otherwise cause silent data loss - an unrecognised license
key, an unreachable collector, a rejected batch - are reported on **stderr** as a
single line, once per condition:

```
[polartrace] collector unavailable: license key not recognised
```

For a full diagnostic trail, point `POLARTRACE_LOG_FILE` at a path you control:

```bash
POLARTRACE_LOG_FILE=/var/log/polartrace-agent.log node -r polartrace server.js
```

A misconfigured agent disables itself and reports why. It will not stop your
application from starting.

## Programmatic usage

Preloading is the recommended integration. When you need explicit control over
configuration, construct the agent yourself:

```js
const { PolarTrace } = require("polartrace");

const agent = new PolarTrace({
  apiKey: process.env.POLARTRACE_LICENSE_KEY,
  serviceName: "checkout-service",
  captureBody: false, // opt out of request-body capture entirely
});

app.use(agent.middleware()); // Express
// agent.instrumentFastify(app);       // Fastify
// app.use(agent.koaMiddleware());     // Koa
```

Call `await agent.shutdown()` to flush anything still queued before exiting.
Under `-r polartrace` this is wired up for you: the agent flushes on `SIGTERM`,
`SIGINT` and `beforeExit`, and defers to your own signal handlers if you have
them.

## Reliability

Telemetry is queued and flushed every 10 seconds, at most 500 items per batch.
Queues are bounded and drop oldest-first, so a collector outage cannot grow
memory without limit. Failed batches are retried with exponential backoff up to
5 minutes and honour the collector's `Retry-After`; batches the collector
rejects outright are dropped rather than retried forever. Timers are `unref`ed,
so the agent never keeps your process alive on its own.

## Examples

[`examples/express-basic`](examples/express-basic) is a minimal runnable Express
app wired up with the agent.

## Support

- Documentation and dashboards: [polartrace.com](https://www.polartrace.com)
- Bugs and feature requests: [GitHub issues](https://github.com/polartrace/polartrace-nodejs-agent/issues)
- Security reports: see [SECURITY.md](SECURITY.md)

## License

[MIT](LICENSE)
