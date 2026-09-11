import Module from "module";
import path from "path";

/**
 * A module-load hook that fires for BOTH `require("pkg")` and
 * `import pkg from "pkg"`.
 *
 * Patching `Module.prototype.require` - the obvious approach - only ever sees
 * CommonJS. An ESM entry point importing a CommonJS package never touches it,
 * so instrumentation installed that way silently does nothing for ESM apps.
 * `Module._load` is reached by both, which is why the hook lives here.
 *
 * The two callers differ in what they pass: CommonJS arrives as the bare
 * specifier ("express"), ESM as an already-resolved absolute path. Both are
 * normalised back to a package name below.
 */
type ModuleHandler = (exports: any) => any | void;

const handlers = new Map<string, ModuleHandler[]>();
const entryCache = new Map<string, string | undefined>();
let installed = false;

/**
 * The package name `request` is the entry point of, or undefined if it is not
 * one. A deep path into a package (`.../express/lib/router/index.js`) is not
 * the package itself and must not be handed to a handler - several of them
 * replace the exports wholesale.
 */
function packageEntryFor(request: string): string | undefined {
  if (!path.isAbsolute(request)) {
    return request.startsWith(".") ? undefined : request;
  }

  if (entryCache.has(request)) return entryCache.get(request);

  let result: string | undefined;
  const marker = `${path.sep}node_modules${path.sep}`;
  const idx = request.lastIndexOf(marker);

  if (idx !== -1) {
    const segments = request.slice(idx + marker.length).split(path.sep);
    const name = segments[0]?.startsWith("@")
      ? `${segments[0]}/${segments[1]}`
      : segments[0];

    // Only resolve for packages somebody actually asked about - `_load` runs
    // for every module in the process and `require.resolve` hits the disk.
    if (name && handlers.has(name)) {
      try {
        const root = request.slice(0, idx + marker.length) + name;
        if (require.resolve(root) === request) result = name;
      } catch {
        // unresolvable - not an entry point we can vouch for
      }
    }
  }

  entryCache.set(request, result);
  return result;
}

/**
 * Run `handler` when `packageName` is loaded, however it is loaded. Returning a
 * value from the handler replaces the module's exports; returning nothing keeps
 * them (for handlers that patch prototypes in place).
 */
export function onModuleLoad(
  packageName: string,
  handler: ModuleHandler,
): void {
  const existing = handlers.get(packageName);
  if (existing) {
    existing.push(handler);
  } else {
    handlers.set(packageName, [handler]);
  }
  // A newly registered package invalidates earlier "not interesting" verdicts.
  entryCache.clear();
  install();
}

function install(): void {
  if (installed) return;
  installed = true;

  const ModuleAny = Module as any;
  const originalLoad = ModuleAny._load;

  ModuleAny._load = function (request: string) {
    const exports = originalLoad.apply(this, arguments as any);

    let name: string | undefined;
    try {
      name = packageEntryFor(request);
    } catch {
      return exports;
    }
    if (!name) return exports;

    const list = handlers.get(name);
    if (!list) return exports;

    let current = exports;
    for (const handler of list) {
      try {
        const replacement = handler(current);
        if (replacement !== undefined) current = replacement;
      } catch {
        // Best effort: a failing hook must never break the host application.
      }
    }
    return current;
  };
}
