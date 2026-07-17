import { getRedis, keys, STATES } from "./redis";

export interface QueueSummary {
  name: string;
  pending: number;
  scheduled: number;
  active: number;
  completed: number;
  dead: number;
  paused: boolean;
}

export interface TaskRecord {
  id: string;
  type?: string;
  payload?: string;
  result?: string;
  queue?: string;
  state?: string;
  maxRetries?: string;
  retryCount?: string;
  lastError?: string;
  createdAt?: string;
  updatedAt?: string;
}

export async function listQueueNames(): Promise<string[]> {
  const r = await getRedis();
  const names = await r.sMembers(keys.queues());
  return names.sort();
}

export async function getQueueSummary(name: string): Promise<QueueSummary> {
  const r = await getRedis();
  const [pending, scheduled, active, completed, dead, paused] = await Promise.all([
    r.lLen(keys.pending(name)),
    r.zCard(keys.scheduled(name)),
    r.zCard(keys.active(name)),
    r.lLen(keys.completed(name)),
    r.lLen(keys.dead(name)),
    r.exists(keys.paused(name)),
  ]);
  return {
    name,
    pending,
    scheduled,
    active,
    completed,
    dead,
    paused: paused === 1,
  };
}

export async function getAllSummaries(): Promise<QueueSummary[]> {
  const names = await listQueueNames();
  return Promise.all(names.map(getQueueSummary));
}

export async function getQueueTasks(
  name: string,
  limit = 25,
): Promise<Record<string, string[]>> {
  const r = await getRedis();
  const [pending, scheduled, active, completed, dead] = await Promise.all([
    r.lRange(keys.pending(name), 0, limit - 1),
    r.zRange(keys.scheduled(name), 0, limit - 1),
    r.zRange(keys.active(name), 0, limit - 1),
    r.lRange(keys.completed(name), 0, limit - 1),
    r.lRange(keys.dead(name), 0, limit - 1),
  ]);
  return { pending, scheduled, active, completed, dead };
}

export async function getTask(id: string): Promise<TaskRecord | null> {
  const r = await getRedis();
  const data = await r.hGetAll(keys.task(id));
  if (!data || Object.keys(data).length === 0) return null;
  return { id, ...data } as TaskRecord;
}

export async function setPaused(name: string, paused: boolean): Promise<void> {
  const r = await getRedis();
  if (paused) {
    await r.set(keys.paused(name), "1");
  } else {
    await r.del(keys.paused(name));
  }
}

// Move dead (or scheduled) tasks back onto the pending list.
export async function requeue(name: string, from: "dead" | "scheduled"): Promise<number> {
  const r = await getRedis();
  const srcKey = from === "dead" ? keys.dead(name) : keys.scheduled(name);
  const ids = from === "dead" ? await r.lRange(srcKey, 0, -1) : await r.zRange(srcKey, 0, -1);
  if (ids.length === 0) return 0;
  const multi = r.multi();
  for (const id of ids) {
    if (from === "dead") {
      multi.lRem(srcKey, 0, id);
    } else {
      multi.zRem(srcKey, id);
    }
    multi.lPush(keys.pending(name), id);
    multi.hSet(keys.task(id), { state: "pending", retryCount: "0" });
  }
  await multi.exec();
  return ids.length;
}

export { STATES };
