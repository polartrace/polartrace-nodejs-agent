/**
 * The agent's own version, reported as `service.version` on emitted spans.
 * Read from the package manifest so it can never drift from what was published:
 * `../package.json` resolves to the package root from both `src/` and `dist/`.
 */
function readAgentVersion(): string {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require("../package.json").version as string;
  } catch {
    return "0.0.0";
  }
}

export const AGENT_VERSION = readAgentVersion();
