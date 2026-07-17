# taskq dashboard

A [Next.js](https://nextjs.org) dashboard for the [taskq](../README.md) task
queue. It connects to the same Redis instance your C++ workers use and shows
live per-queue counts, with controls to pause/resume queues and requeue
dead-lettered tasks.

## Run

```bash
cd web
npm install
npm run dev
```

Open http://localhost:3000. The page polls `/api/queues` every 2 seconds.

## Configuration

Set the Redis connection via environment variable (defaults to
`redis://127.0.0.1:6379/0`):

```bash
TASKQ_REDIS_URL=redis://localhost:6379/0 npm run dev
```

## API routes

| Route | Method | Purpose |
| --- | --- | --- |
| `/api/queues` | GET | Summaries (per-state counts) for every queue. |
| `/api/queues/[name]` | GET | Summary + sample task ids per state for one queue. |
| `/api/queues/[name]/pause` | POST | Body `{ "paused": true \| false }`. |
| `/api/queues/[name]/requeue` | POST | Body `{ "from": "dead" \| "scheduled" }`. |
| `/api/tasks/[id]` | GET | Full record for a single task. |

All queue logic lives in [`src/lib/queue.ts`](src/lib/queue.ts); the Redis key
layout mirrors `taskq.hpp` and is defined in [`src/lib/redis.ts`](src/lib/redis.ts).
