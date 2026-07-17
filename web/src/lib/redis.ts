import { createClient, type RedisClientType } from "redis";

// A single shared Redis connection, reused across requests. Next.js may reload
// modules in dev, so we cache the client on globalThis to avoid leaking
// connections on every hot reload.
const globalForRedis = globalThis as unknown as {
  taskqClient?: RedisClientType;
};

export async function getRedis(): Promise<RedisClientType> {
  if (globalForRedis.taskqClient?.isOpen) {
    return globalForRedis.taskqClient;
  }
  const url = process.env.TASKQ_REDIS_URL ?? "redis://127.0.0.1:6379/0";
  const client: RedisClientType = createClient({ url });
  client.on("error", (err) => console.error("taskq redis error:", err));
  await client.connect();
  globalForRedis.taskqClient = client;
  return client;
}

// Mirror of the key layout defined in taskq.hpp.
export const keys = {
  queues: () => "taskq:queues",
  pending: (q: string) => `taskq:queue:${q}:pending`,
  active: (q: string) => `taskq:queue:${q}:active`,
  scheduled: (q: string) => `taskq:queue:${q}:scheduled`,
  completed: (q: string) => `taskq:queue:${q}:completed`,
  dead: (q: string) => `taskq:queue:${q}:dead`,
  paused: (q: string) => `taskq:queue:${q}:paused`,
  task: (id: string) => `taskq:task:${id}`,
};

export const STATES = ["pending", "scheduled", "active", "completed", "dead"] as const;
export type StateName = (typeof STATES)[number];
