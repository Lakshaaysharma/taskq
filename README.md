# taskq

**A distributed, Redis-backed task queue for modern C++17** — header-only, with
retries, scheduling, priorities, crash recovery, and companion tooling (a CLI
and a web dashboard).

Enqueue background jobs from anywhere, run them asynchronously across a pool of
workers, and let Redis keep the state durable. If a worker dies mid-task, taskq
notices the expired lease and re-runs the job elsewhere.

## Table of Contents
- [Why taskq](#why-taskq)
- [Architecture](#architecture)
- [Features](#features)
- [Requirements](#requirements)
- [Quickstart](#quickstart)
- [Example](#example)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Testing](#testing)
- [CLI](#cli)
- [Web dashboard](#web-dashboard)
- [Project layout](#project-layout)
- [License](#license)

## Why taskq
Background work — sending email, resizing images, generating reports — should
not block the request that triggered it. taskq gives a C++ service the same
enqueue/worker model that Sidekiq or Celery bring to other ecosystems, in a
single header with no framework to adopt.

- **Header-only.** Drop in `taskq.hpp`, link hiredis + pthread, done.
- **Durable.** Every task is a Redis hash; queue transitions are atomic Lua.
- **Resilient.** Crashed workers don't lose work — leases expire and recover.
- **Observable.** A CLI and a live web dashboard ship in the box.

## Architecture
```
   producer(s)                    Redis                      worker server
 ┌────────────┐          ┌────────────────────┐          ┌──────────────────┐
 │ enqueue()  │ ───────► │ pending  (list)    │ ───────► │ dispatch loop     │
 │            │          │ scheduled(zset)    │          │  └► thread pool   │
 └────────────┘          │ active   (zset)    │ ◄─┐      │      └► handler   │
                         │ completed(list)    │   │      └──────────────────┘
                         │ dead     (list)    │   └─ lease expiry → recovery
                         │ task:<id>(hash)    │
                         └────────────────────┘
```
Producers push task ids onto per-queue lists. The server's dispatch loop
promotes due scheduled tasks, recovers abandoned ones, then leases pending
tasks to a thread pool. Each handler runs; on success the task is completed, on
exception it is retried with exponential backoff, and once retries are
exhausted it is buried in the dead-letter list.

## Features
- **At-least-once delivery** with per-task retry budgets.
- **Exactly-once side effects** via idempotency keys — a duplicate or redelivered
  task whose key already completed is a no-op that returns the original result.
- **Exponential backoff with jitter** on failure (delay uniform in `[d/2, d]`,
  `d = base * 2^retry`) to avoid a thundering herd of simultaneous retries.
- **Scheduled tasks** — run at a wall-clock time or after a delay.
- **Weighted priorities** — higher-weight queues are polled more often.
- **Crash recovery** — leased tasks whose worker dies are automatically requeued.
- **Dead-letter queue** for tasks that exhaust their retries.
- **Pause / resume** individual queues without stopping workers.
- **Connection reuse** — one long-lived Redis connection per worker thread, with
  an event-driven prefetch window for high throughput.
- **No heavyweight deps** — hiredis + pthread; UUIDs and the thread pool are built in.

## Benchmarks
Measured by [`bench.cpp`](bench.cpp) on a local Redis (Apple Silicon, 8 workers).
Numbers are machine-dependent; run it yourself with `./bench`.

| Metric | Value |
| --- | --- |
| Throughput (drain 50k tasks) | **~10,000 jobs/sec** |
| Enqueue→handler latency, p50 | ~1.4 ms |
| Enqueue→handler latency, p95 | ~2.3 ms |
| Enqueue→handler latency, p99 | ~2.5 ms |

```bash
g++ -std=c++17 -O2 bench.cpp -I. -I$(brew --prefix hiredis)/include \
    -L$(brew --prefix hiredis)/lib -lhiredis -lpthread -o bench
./bench            # or: ./bench <tasks> <workers>
```

## Requirements
- A C++17 compiler (`g++` or `clang++`).
- [hiredis](https://github.com/redis/hiredis).
- Redis 6.0+ reachable (default `127.0.0.1:6379`).
- The example additionally uses [nlohmann/json](https://github.com/nlohmann/json) for payloads (optional in your own code — payloads are opaque strings).

Install dependencies:

```bash
# macOS (Homebrew)
brew install hiredis nlohmann-json redis catch2

# Debian / Ubuntu
sudo apt install g++ libhiredis-dev nlohmann-json3-dev catch2 redis-server
```

## Quickstart
1. Copy `taskq.hpp` into your include path.
2. Compile against hiredis and pthread:
   ```bash
   g++ -std=c++17 your_app.cpp -I. -lhiredis -lpthread -o app
   ```
3. Register handlers, enqueue tasks, and run the server.

## Example
```cpp
#include "taskq.hpp"

const std::string TypeEmail = "email:deliver";

void HandleEmail(taskq::Task& task) {
  // task.payload holds whatever string you enqueued (JSON here, but anything works)
  // ... do the work ...
  task.result = "{\"sent\":true}";   // recorded on success
  // throw an exception to signal failure and trigger a retry
}

int main() {
  taskq::registerHandler(TypeEmail, &HandleEmail);

  redisContext* c = redisConnect("127.0.0.1", 6379);

  // Run now, on the "default" queue, with up to 5 retries.
  taskq::Task t{TypeEmail, "{\"userId\":42}", 5};
  taskq::enqueue(c, t, "default");

  // Run 30 seconds from now on a high-priority queue.
  taskq::Task later{TypeEmail, "{\"userId\":7}", 5};
  taskq::enqueue(c, later, "high", taskq::runIn(std::chrono::seconds(30)));

  redisFree(c);

  // Start workers. Weights make "high" polled 4x as often as "low".
  taskq::ServerConfig cfg;
  cfg.queues = {{"low", 1}, {"default", 2}, {"high", 4}};
  cfg.concurrency = 8;
  taskq::runServer(cfg);   // blocks; call server.stop() from a signal handler
}
```

Build and run the bundled demo:

```bash
g++ -std=c++17 example.cpp -I. \
    -I$(brew --prefix hiredis)/include \
    -I$(brew --prefix nlohmann-json)/include \
    -L$(brew --prefix hiredis)/lib -lhiredis -lpthread -o example
./example
```

## Configuration
`taskq::ServerConfig` controls the worker server:

| Field | Default | Meaning |
| --- | --- | --- |
| `host` / `port` | `127.0.0.1` / `6379` | Redis connection. |
| `queues` | `{{"default", 1}}` | Queue name → poll weight. |
| `concurrency` | hardware threads | Worker thread count. |
| `leaseDuration` | 30s | How long a task may run before it's deemed lost. |
| `pollInterval` | 500ms | Dispatch loop idle sleep / housekeeping cadence. |
| `retryBackoff` | 1s | Base delay; actual delay is jittered within `[d/2, d]`, `d = base * 2^retry`. |
| `prefetch` | 16 | Tasks kept in flight per worker (`concurrency * prefetch`). |
| `idempotencyTTL` | 0 (forever) | Lifetime of idempotency markers. |
| `historyLimit` | 1000 | Completed/dead ids kept per queue. |

### Idempotency (exactly-once side effects)
At-least-once delivery means a task can run more than once (e.g. a worker crashes
after doing the work but before recording success, so the task is redelivered).
Attach an idempotency key and taskq guarantees the side effect happens at most
once per key — a duplicate or redelivered task becomes a no-op that returns the
original result:

```cpp
taskq::Task charge{TypeChargeCard, payload, 5};
taskq::enqueue(c, charge, "payments", taskq::idempotent("order-4271"));
// A second enqueue with key "order-4271" will not re-run the handler.
```

## How it works
Each queue is a set of Redis structures under `taskq:queue:<name>:*`, and every
task is a hash at `taskq:task:<id>`. Because a naive "read then write" sequence
would race under concurrency, every state transition — lease, complete, fail,
promote-scheduled, recover — is a single Lua script evaluated atomically inside
Redis. That keeps the core invariant intact: **a task is in exactly one bucket
at a time**, even with many producers and workers.

Crash recovery works through *leases*: when a task is dispatched it's added to
the `active` sorted set scored with a deadline. If the worker finishes, the
entry is removed. If the worker dies, the deadline passes and the next dispatch
loop moves the task back to `pending`.

## Testing
The suite (Catch2 v3) exercises enqueue, scheduling, retries, dead-lettering,
recovery, pause logic, and the thread pool — including end-to-end runs against a
live Redis.

```bash
redis-server --daemonize yes      # or: brew services start redis
./build_tests.sh
./tests                           # ./tests '[retry]' to filter by tag
```

Tags: `[queue]`, `[schedule]`, `[retry]`, `[recovery]`, `[pause]`,
`[idempotency]`, `[threadpool]`, `[e2e]`.

## CLI
A Python operations tool lives in [`cli/`](cli/). Inspect queues, drill into
tasks, pause/resume, and requeue dead work:

```bash
cd cli && pip install -r requirements.txt
python3 taskqctl.py queues
python3 taskqctl.py task <id> --json
python3 taskqctl.py requeue default --state dead
```

See [`cli/README.md`](cli/README.md).

## Web dashboard
A Next.js dashboard in [`web/`](web/) shows live per-queue counts and offers
pause/resume and requeue controls:

```bash
cd web && npm install && npm run dev
# http://localhost:3000
```

See [`web/README.md`](web/README.md).

## Project layout
```
.
├── taskq.hpp         # Header-only task queue library
├── example.cpp       # Producer + worker demo
├── bench.cpp         # Throughput + latency benchmark
├── tests.cpp         # Catch2 regression suite
├── build_tests.sh    # Test build helper
├── format.sh         # clang-format helper
├── cli/              # Python operations CLI (taskqctl)
└── web/              # Next.js monitoring dashboard
```

## License
Released under the [MIT License](LICENSE). This is an original implementation
written from scratch.
