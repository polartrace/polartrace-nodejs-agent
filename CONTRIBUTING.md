# Contributing

Thanks for your interest in improving the Polartrace Node.js agent!

## Development setup

Fork and clone the repository:

```bash
git clone git@github.com:your-username/polartrace-nodejs-agent.git
cd polartrace-nodejs-agent
npm install
```

## Working on the agent

- Source lives in `src/`; the published package ships the compiled `dist/`.
- `npm run build` compiles with `tsc`; `npm run dev` watches.
- `npm run lint` runs ESLint over `src/`.
- To try your changes against a real app, use `examples/express-basic` and point
  `POLARTRACE_ENDPOINT` at your collector.
- The agent must never write to stdout or take the host application down. Route
  its own output through `src/diagnostics.ts`: `error`/`warn` for conditions an
  operator has to act on, `debug` for everything else. Use `warnOnce` for
  anything that can recur on every flush tick.

## Before submitting a pull request

1. Run `npm run lint` and fix any issues
2. Run `npm run build` and make sure it succeeds
3. Describe what changed and why in the PR body; update `CHANGELOG.md` under `[Unreleased]`

## Reporting bugs

Open a GitHub issue with the agent version, Node.js version, framework
(Express/Mongo/Redis versions if relevant), and a minimal reproduction.
For security issues see [SECURITY.md](SECURITY.md) - do not open a public issue.
