# polartrace

Package for monitoring and logging middleware for PolarTrace observability platform.

## Installation

```bash
npm install polartrace
```

## Quick Start

PolarTrace agent works automatically with zero code changes. Simply use environment variables and require the agent:

```bash
POLARTRACE_APP_NAME=<SERVICE_NAME> POLARTRACE_LICENSE_KEY=<API_KEY> node -r polartrace <main-file-name>.js
```

The agent will automatically:

- Initialize with your API key and service name from environment variables
- Attach middleware to Express applications
- Start collecting logs and traces
- Send data to the PolarTrace collector

### Docker Example

```dockerfile
FROM node:18

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .

# Set environment variables
ENV POLARTRACE_APP_NAME=my-service
ENV POLARTRACE_LICENSE_KEY=your_api_key_here

# Run with PolarTrace agent
CMD ["node", "-r", "polartrace", "server.js"]
```

Or using docker-compose:

```yaml
services:
  app:
    build: .
    environment:
      - POLARTRACE_APP_NAME=my-service
      - POLARTRACE_LICENSE_KEY=your_api_key_here
    command: node -r polartrace server.js
```

## Configuration

All configuration is done via environment variables. No code changes required!

### Required Environment Variables

- `POLARTRACE_APP_NAME`: Your service name (must be unique within your organization)
- `POLARTRACE_LICENSE_KEY`: Your PolarTrace API key

### Optional Environment Variables

- `POLARTRACE_ENABLE_CONSOLE_LOG`: Set to `"true"` to enable console logging for debugging
- `POLARTRACE_ENDPOINT`: Override the collector URL (default: `https://collector.polartrace.com/api/log`)
- `POLARTRACE_DISABLE_HOST_METRICS`: Set to `"1"`/`"true"` to disable CPU/memory/event-loop host metrics
- `POLARTRACE_DISABLE_MONGO_SPANS`: Set to `"1"`/`"true"` to disable MongoDB span collection
- `POLARTRACE_DISABLE_REDIS_SPANS`: Set to `"1"`/`"true"` to disable Redis span collection
- `POLARTRACE_DISABLE_POSTGRES_SPANS`: Set to `"1"`/`"true"` to disable PostgreSQL span collection

### Default Behavior

- **Capture Headers**: Enabled by default
- **Capture Body**: Enabled by default (sensitive fields are automatically redacted)
- **Capture Query Parameters**: Enabled by default
- **Capture Console Logs**: Enabled by default
- **MongoDB Span Collection**: Enabled by default
- **Redis Span Collection**: Enabled by default
- **PostgreSQL Span Collection** (`pg` driver): Enabled by default
- **Host Metrics** (CPU, memory, event-loop lag): Enabled by default

### Agent Log File

The agent writes its own diagnostics to `polartrace_agent.log` in your process's
working directory (append-only). Add it to your `.gitignore`.

## Examples

See [`examples/express-basic`](examples/express-basic) for a minimal runnable Express app.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, see [SECURITY.md](SECURITY.md).
