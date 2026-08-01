import { createRequire } from "module";
import * as path from "path";

const TS_ENTRY_PATTERN = /\.(ts|mts|cts|tsx)$/i;

function loadFromHost(moduleId: string): any | null {
  // createRequire anchored at the host app's cwd lets Node walk the host's
  // node_modules tree, including hoisted packages in monorepos.
  const anchor = path.join(process.cwd(), "package.json");
  const hostRequire = createRequire(anchor);
  try {
    return hostRequire(moduleId);
  } catch {
    return null;
  }
}

export function registerTypeScriptLoaderIfNeeded(): void {
  const entry = process.argv[1] || "";
  if (!TS_ENTRY_PATTERN.test(entry)) {
    return;
  }

  // Prefer tsx (esbuild-based, faster) when available, fall back to ts-node.
  const tsx = loadFromHost("tsx/cjs/api");
  if (tsx && typeof tsx.register === "function") {
    tsx.register();
    return;
  }

  const tsNode = loadFromHost("ts-node");
  if (tsNode && typeof tsNode.register === "function") {
    // transpileOnly avoids type-checking on startup so behavior matches a
    // plain `node app.js` run - faster startup, no surprise type errors.
    // skipProject so apps without a tsconfig (or with one that excludes the
    // entry file) still run; defaults are sensible enough for modern code.
    tsNode.register({
      transpileOnly: true,
      compilerOptions: {
        module: "commonjs",
        target: "ES2020",
        esModuleInterop: true,
        resolveJsonModule: true,
      },
    });
    return;
  }

  // Neither loader present: emit a clear, actionable error. We do NOT throw
  // here - Node will fail on its own when it tries to parse the .ts file,
  // and our message tells the user how to fix it.
  process.stderr.write(
    `[polartrace] Cannot run TypeScript entry "${entry}" - install ts-node or tsx as a dependency of your app:\n` +
      `  npm install --save-dev ts-node typescript\n`,
  );
}
