import { Instrumentation } from "./instrumentation";
import { MongoInstrumentation } from "./mongo.instrumentation";
import { RedisInstrumentation } from "./redis.instrumentation";
import { PostgresInstrumentation } from "./postgres.instrumentation";

class InstrumentationManager {
    private static instance: InstrumentationManager;
    private instrumentations: Instrumentation[] = [];

    static getInstance() {
        if (!this.instance) {
            this.instance = new InstrumentationManager();
        }
        return this.instance;
    }

    register(inst: Instrumentation) {
        this.instrumentations.push(inst);
    }

    /**
     * Find an existing instrumentation by name and update its callback if it's a MongoInstrumentation
     */
    updateMongoCallback(callback: (span: any) => void): boolean {
        const mongoInst = this.instrumentations.find(
            (inst) => inst instanceof MongoInstrumentation
        ) as MongoInstrumentation | undefined;

        if (mongoInst) {
            mongoInst.setOnSpan(callback);
            return true;
        }
        return false;
    }

    /**
     * Find an existing RedisInstrumentation and update its callback so the
     * pre-registered (placeholder) instrumentation from register.ts can be
     * rewired to the real Polartrace pipeline once the agent boots.
     */
    updateRedisCallback(callback: (span: any) => void): boolean {
        const redisInst = this.instrumentations.find(
            (inst) => inst instanceof RedisInstrumentation
        ) as RedisInstrumentation | undefined;

        if (redisInst) {
            redisInst.setOnSpan(callback);
            return true;
        }
        return false;
    }

    /**
     * Find an existing PostgresInstrumentation and update its callback so the
     * pre-registered (placeholder) instrumentation from register.ts can be
     * rewired to the real Polartrace pipeline once the agent boots.
     */
    updatePostgresCallback(callback: (span: any) => void): boolean {
        const pgInst = this.instrumentations.find(
            (inst) => inst instanceof PostgresInstrumentation
        ) as PostgresInstrumentation | undefined;

        if (pgInst) {
            pgInst.setOnSpan(callback);
            return true;
        }
        return false;
    }

    enableAll() {
        for (const inst of this.instrumentations) {
            inst.enable();
        }
    }
}

export { InstrumentationManager };
