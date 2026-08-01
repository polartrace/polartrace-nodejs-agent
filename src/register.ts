// Pre-register custom instrumentations when this module is preloaded via
// `node -r polartrace`. This guarantees driver modules (mongodb / ioredis /
// redis) are patched BEFORE the host application requires them.
//
// Each instrumentation is registered with a no-op callback here; the
// PolarTrace constructor in index.ts rewires both to the real pipeline via
// `InstrumentationManager.update{Mongo,Redis}Callback` once the agent boots.

// IMPORTANT: register the TypeScript loader FIRST, before any other agent
// code runs. When the user runs `node -r polartrace app.ts`, Node cannot
// parse the .ts entry on its own - we have to install a loader during the
// preload phase so the entry file resolves successfully.
import { registerTypeScriptLoaderIfNeeded } from "./ts-loader";
registerTypeScriptLoaderIfNeeded();

import { InstrumentationManager } from "./instrumentations/instrumentation-manager";
import { MongoInstrumentation } from "./instrumentations/mongo.instrumentation";
import { RedisInstrumentation } from "./instrumentations/redis.instrumentation";

const manager = InstrumentationManager.getInstance();

manager.register(new MongoInstrumentation(() => {}));
manager.register(new RedisInstrumentation(() => {}));

manager.enableAll();

// Status-summary flags (read by showConnectionStatus in index.ts)
(globalThis as any).__POLARTRACE_MONGO_INSTRUMENTATION_ENABLED__ = true;
(globalThis as any).__POLARTRACE_REDIS_INSTRUMENTATION_ENABLED__ = true;
