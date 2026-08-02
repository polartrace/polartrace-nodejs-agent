# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.12] - 2026-08-01

### Added
- PostgreSQL span collection via `pg` require hooking — query spans with `db.system`/`db.operation`/`db.sql.table`/`db.statement` attributes (opt-out: `POLARTRACE_DISABLE_POSTGRES_SPANS`)
- Redis span collection via `ioredis`/`redis` require hooking (opt-out: `POLARTRACE_DISABLE_REDIS_SPANS`)
- Host metrics sampling: process CPU/memory, event-loop lag percentiles, host load/memory, container cgroup limits (opt-out: `POLARTRACE_DISABLE_HOST_METRICS`)
- `POLARTRACE_ENDPOINT` environment variable to override the collector URL

### Changed
- Default collector endpoint is `https://collector.polartrace.com/api/log`
- `mongodb`/`mongoose` are no longer hard dependencies — instrumentation patches the host application's own copy (`mongoose` is now an optional peer dependency)

## [1.0.0] - 2026-02-01

### Added
- Initial release of PolarTrace Node.js agent
- Zero-code integration via `-r polartrace` module preloading
- Automatic Express middleware instrumentation
- Custom MongoDB query instrumentation via require hooking
- OpenTelemetry-compliant trace collection and monitoring
- Sensitive data sanitization (passwords, tokens, secrets, credit cards, SSN)
- Request/response metadata capture (headers, query params, body)
- Console log capture per request
- Error stack trace collection and reporting
- Queue-based batching with 10-second flush intervals
- Automatic retry mechanism for failed API calls
- File logging to polartrace_agent.log
- Support for HTTPS endpoints
- Environment variable configuration (POLARTRACE_LICENSE_KEY & POLARTRACE_APP_NAME)
- Parent-child span relationship tracking
- Trace grouping and correlation

### Security
- API key validation (minimum length check)
- Service name and endpoint validation
- Sensitive field redaction in request bodies and span attributes
- Internal access token validation
- Recursive object sanitization for nested sensitive data
