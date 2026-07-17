// taskq — a distributed, Redis-backed task queue for modern C++17.
//
// Header-only. Drop this file into your include path, link hiredis and
// pthread, and you have a resilient background-job system: producers enqueue
// tasks, a pool of workers pulls them by priority, runs your handlers, and
// records the outcome back in Redis. Failed tasks are retried with backoff;
// tasks whose worker dies mid-flight are recovered and re-run.
//
// Copyright (c) 2026 Lakshay Sharma. Released under the MIT License.

#ifndef TASKQ_HPP
#define TASKQ_HPP

#include <hiredis/hiredis.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace taskq {

// ---------------------------------------------------------------------------
// Key namespace
//
// Everything taskq stores in Redis lives under the "taskq:" prefix so it can
// coexist with other data. The layout is:
//
//   taskq:queues                       SET   of known queue names
//   taskq:queue:<q>:pending            LIST  task ids waiting to run (FIFO)
//   taskq:queue:<q>:active             ZSET  in-flight ids, score = lease deadline (ms)
//   taskq:queue:<q>:scheduled          ZSET  future ids, score = run-at time (ms)
//   taskq:queue:<q>:completed          LIST  ids of succeeded tasks (capped)
//   taskq:queue:<q>:dead               LIST  ids that exhausted their retries
//   taskq:queue:<q>:paused             STR   presence means "do not dispatch"
//   taskq:task:<id>                    HASH  the task record itself
//   taskq:idem:<q>:<key>               STR   idempotency marker (result of a
//                                            completed key; presence => no-op)
// ---------------------------------------------------------------------------
namespace keys {
inline std::string queues() { return "taskq:queues"; }
inline std::string pending(const std::string& q) { return "taskq:queue:" + q + ":pending"; }
inline std::string active(const std::string& q) { return "taskq:queue:" + q + ":active"; }
inline std::string scheduled(const std::string& q) { return "taskq:queue:" + q + ":scheduled"; }
inline std::string completed(const std::string& q) { return "taskq:queue:" + q + ":completed"; }
inline std::string dead(const std::string& q) { return "taskq:queue:" + q + ":dead"; }
inline std::string paused(const std::string& q) { return "taskq:queue:" + q + ":paused"; }
inline std::string task(const std::string& id) { return "taskq:task:" + id; }
inline std::string idem(const std::string& q, const std::string& key) {
  return "taskq:idem:" + q + ":" + key;
}
}  // namespace keys

// ---------------------------------------------------------------------------
// Task model
// ---------------------------------------------------------------------------
enum class State {
  Pending,    // waiting in a queue to be dispatched
  Scheduled,  // waiting for its run-at time to arrive
  Active,     // leased to a worker and running
  Completed,  // handler returned successfully
  Dead        // exhausted all retries
};

inline std::string toString(State s) {
  switch (s) {
    case State::Pending: return "pending";
    case State::Scheduled: return "scheduled";
    case State::Active: return "active";
    case State::Completed: return "completed";
    case State::Dead: return "dead";
  }
  return "unknown";
}

inline State stateFromString(const std::string& s) {
  if (s == "pending") return State::Pending;
  if (s == "scheduled") return State::Scheduled;
  if (s == "active") return State::Active;
  if (s == "completed") return State::Completed;
  return State::Dead;
}

struct Task {
  std::string id;         // unique identifier (assigned at enqueue)
  std::string type;       // handler key, e.g. "email:deliver"
  std::string payload;    // opaque user data, typically JSON
  std::string result;     // set by the handler on success
  std::string queue;      // queue the task belongs to
  State state = State::Pending;
  int maxRetries = 0;     // how many times to retry on failure
  int retryCount = 0;     // retries used so far
  std::string lastError;  // exception message from the last failed attempt
  int64_t createdAt = 0;  // epoch millis
  int64_t updatedAt = 0;  // epoch millis

  // Optional idempotency key. When set, the handler's side effect runs at most
  // once per (queue, key): a redelivered or duplicate task with a key that has
  // already completed is a no-op that returns the original result.
  std::string idempotencyKey;

  Task() = default;
  Task(std::string type_, std::string payload_, int maxRetries_)
      : type(std::move(type_)), payload(std::move(payload_)), maxRetries(maxRetries_) {}
};

// A handler is any callable that takes the task by reference (so it can set
// `result`) and either returns normally (success) or throws (failure).
using Handler = std::function<void(Task&)>;

// ---------------------------------------------------------------------------
// Enqueue options
// ---------------------------------------------------------------------------
struct EnqueueOptions {
  // If set, the task is placed on the scheduled ZSET and only becomes eligible
  // once this time is reached. Defaults to "run immediately".
  std::chrono::system_clock::time_point runAt = std::chrono::system_clock::time_point::min();
  bool scheduled = false;

  // Optional idempotency key (see Task::idempotencyKey).
  std::string idempotencyKey;
};

inline EnqueueOptions runAt(std::chrono::system_clock::time_point when) {
  EnqueueOptions o;
  o.runAt = when;
  o.scheduled = true;
  return o;
}

inline EnqueueOptions runIn(std::chrono::milliseconds delay) {
  return runAt(std::chrono::system_clock::now() + delay);
}

inline EnqueueOptions idempotent(const std::string& key) {
  EnqueueOptions o;
  o.idempotencyKey = key;
  return o;
}

// ---------------------------------------------------------------------------
// Small internal utilities
// ---------------------------------------------------------------------------
namespace detail {

inline int64_t nowMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline int64_t toMillis(std::chrono::system_clock::time_point tp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

// "Equal jitter" backoff (AWS-style): returns a value uniformly in
// [base/2, base]. Spreading retries prevents a thundering herd of failed tasks
// all retrying at the same instant.
inline int64_t jitter(int64_t base) {
  if (base <= 1) return base;
  static thread_local std::mt19937_64 rng(
      std::random_device{}() ^
      static_cast<uint64_t>(
          std::chrono::high_resolution_clock::now().time_since_epoch().count()));
  std::uniform_int_distribution<int64_t> d(0, base / 2);
  return base / 2 + d(rng);
}

// A compact UUID v4 generator so we don't depend on libuuid. Not intended to
// be cryptographically strong — task ids only need to be unique.
inline std::string generateId() {
  static thread_local std::mt19937_64 rng(
      std::random_device{}() ^
      static_cast<uint64_t>(
          std::chrono::high_resolution_clock::now().time_since_epoch().count()));
  std::uniform_int_distribution<uint32_t> hex(0, 15);
  std::uniform_int_distribution<uint32_t> variant(8, 11);
  const char* digits = "0123456789abcdef";
  std::string out;
  out.reserve(36);
  for (int i = 0; i < 36; ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      out.push_back('-');
    } else if (i == 14) {
      out.push_back('4');  // version 4
    } else if (i == 19) {
      out.push_back(digits[variant(rng)]);
    } else {
      out.push_back(digits[hex(rng)]);
    }
  }
  return out;
}

// RAII wrapper around a hiredis reply so we never leak on early return / throw.
class Reply {
 public:
  explicit Reply(void* r) : reply_(static_cast<redisReply*>(r)) {}
  ~Reply() {
    if (reply_) freeReplyObject(reply_);
  }
  Reply(const Reply&) = delete;
  Reply& operator=(const Reply&) = delete;
  Reply(Reply&& o) noexcept : reply_(o.reply_) { o.reply_ = nullptr; }

  redisReply* get() const { return reply_; }
  redisReply* operator->() const { return reply_; }
  explicit operator bool() const { return reply_ != nullptr; }

 private:
  redisReply* reply_;
};

// Issue a command and return its reply wrapped for cleanup. Throws on a null
// reply (connection-level failure) so callers can rely on a valid object.
inline Reply command(redisContext* c, const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  void* raw = redisvCommand(c, fmt, ap);
  va_end(ap);
  if (raw == nullptr) {
    throw std::runtime_error(std::string("taskq: redis command failed: ") +
                             (c ? c->errstr : "null context"));
  }
  return Reply(raw);
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Lua scripts
//
// The queue's invariants (a task is in exactly one place, retries increment
// atomically, a paused queue never dispatches) can't be guaranteed with a
// sequence of separate commands under concurrency. Each state transition is a
// single EVAL so it happens atomically on the Redis side.
// ---------------------------------------------------------------------------
namespace lua {

// Move due scheduled tasks into the pending list. Returns the count promoted.
// KEYS[1]=scheduled zset  KEYS[2]=pending list   ARGV[1]=now(ms)
inline const char* kPromoteScheduled = R"LUA(
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('LPUSH', KEYS[2], id)
  redis.call('HSET', 'taskq:task:' .. id, 'state', 'pending', 'updatedAt', ARGV[1])
end
return #due
)LUA";

// Atomically lease the next pending task to a worker, unless the queue is
// paused. Returns the task id, or nil if paused / empty.
// KEYS[1]=paused flag  KEYS[2]=pending list  KEYS[3]=active zset
// ARGV[1]=lease deadline(ms)  ARGV[2]=now(ms)
inline const char* kDequeue = R"LUA(
if redis.call('EXISTS', KEYS[1]) == 1 then return nil end
local id = redis.call('RPOP', KEYS[2])
if not id then return nil end
redis.call('ZADD', KEYS[3], ARGV[1], id)
redis.call('HSET', 'taskq:task:' .. id, 'state', 'active', 'updatedAt', ARGV[2])
return id
)LUA";

// Mark a leased task as completed. If an idempotency marker key is supplied,
// record the result under it (atomically with completion) so future tasks
// sharing that key become no-ops.
// KEYS[1]=active zset  KEYS[2]=completed list
// ARGV[1]=id  ARGV[2]=result  ARGV[3]=now(ms)  ARGV[4]=history cap
// ARGV[5]=idempotency marker key ('' = none)  ARGV[6]=marker TTL ms (0 = persist)
inline const char* kComplete = R"LUA(
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', 'taskq:task:' .. ARGV[1], 'state', 'completed', 'result', ARGV[2], 'updatedAt', ARGV[3])
redis.call('LPUSH', KEYS[2], ARGV[1])
redis.call('LTRIM', KEYS[2], 0, tonumber(ARGV[4]) - 1)
if ARGV[5] ~= '' then
  if tonumber(ARGV[6]) > 0 then
    redis.call('SET', ARGV[5], ARGV[2], 'PX', ARGV[6])
  else
    redis.call('SET', ARGV[5], ARGV[2])
  end
end
return 1
)LUA";

// Handle a failed attempt: retry if budget remains, otherwise bury as dead.
// KEYS[1]=active zset  KEYS[2]=pending list  KEYS[3]=scheduled zset  KEYS[4]=dead list
// ARGV[1]=id  ARGV[2]=error  ARGV[3]=now(ms)  ARGV[4]=retry-at(ms)  ARGV[5]=history cap
// Returns 'retried' or 'dead'.
inline const char* kFail = R"LUA(
redis.call('ZREM', KEYS[1], ARGV[1])
local hkey = 'taskq:task:' .. ARGV[1]
local retry = tonumber(redis.call('HGET', hkey, 'retryCount')) or 0
local maxr = tonumber(redis.call('HGET', hkey, 'maxRetries')) or 0
redis.call('HSET', hkey, 'lastError', ARGV[2], 'updatedAt', ARGV[3])
if retry < maxr then
  redis.call('HSET', hkey, 'retryCount', retry + 1, 'state', 'scheduled')
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
  return 'retried'
else
  redis.call('HSET', hkey, 'state', 'dead')
  redis.call('LPUSH', KEYS[4], ARGV[1])
  redis.call('LTRIM', KEYS[4], 0, tonumber(ARGV[5]) - 1)
  return 'dead'
end
)LUA";

// Requeue tasks whose lease expired (worker presumed dead). Returns the count.
// KEYS[1]=active zset  KEYS[2]=pending list  ARGV[1]=now(ms)
inline const char* kRecover = R"LUA(
local stuck = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(stuck) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('LPUSH', KEYS[2], id)
  redis.call('HSET', 'taskq:task:' .. id, 'state', 'pending', 'updatedAt', ARGV[1])
end
return #stuck
)LUA";

}  // namespace lua

// ---------------------------------------------------------------------------
// Thread pool
//
// A minimal fixed-size pool: tasks are submitted as std::function jobs and run
// on N worker threads. Written from scratch to keep taskq dependency-free.
// ---------------------------------------------------------------------------
class ThreadPool {
 public:
  explicit ThreadPool(size_t threads) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] { workerLoop(); });
    }
  }

  ~ThreadPool() { shutdown(); }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void submit(std::function<void()> job) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) return;
      jobs_.push(std::move(job));
      ++outstanding_;
    }
    cv_.notify_one();
  }

  // Number of jobs queued or currently running.
  size_t outstanding() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return outstanding_;
  }

  size_t size() const { return workers_.size(); }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) return;
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto& w : workers_) {
      if (w.joinable()) w.join();
    }
  }

 private:
  void workerLoop() {
    for (;;) {
      std::function<void()> job;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return stopping_ || !jobs_.empty(); });
        if (stopping_ && jobs_.empty()) return;
        job = std::move(jobs_.front());
        jobs_.pop();
      }
      job();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        --outstanding_;
      }
    }
  }

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> jobs_;
  std::vector<std::thread> workers_;
  size_t outstanding_ = 0;
  bool stopping_ = false;
};

// ---------------------------------------------------------------------------
// Handler registry (process-global, as in the typical single-binary worker)
// ---------------------------------------------------------------------------
namespace detail {
inline std::unordered_map<std::string, Handler>& registry() {
  static std::unordered_map<std::string, Handler> handlers;
  return handlers;
}
}  // namespace detail

inline void registerHandler(const std::string& type, Handler handler) {
  detail::registry()[type] = std::move(handler);
}

// ---------------------------------------------------------------------------
// Producer API
// ---------------------------------------------------------------------------

// Persist a task's hash record. Used internally by enqueue.
namespace detail {
inline void writeTaskHash(redisContext* c, const Task& t) {
  Reply r = command(
      c,
      "HSET %s id %s type %s payload %s result %s queue %s state %s "
      "maxRetries %d retryCount %d lastError %s createdAt %lld updatedAt %lld "
      "idempotencyKey %s",
      keys::task(t.id).c_str(), t.id.c_str(), t.type.c_str(), t.payload.c_str(),
      t.result.c_str(), t.queue.c_str(), toString(t.state).c_str(), t.maxRetries,
      t.retryCount, t.lastError.c_str(), static_cast<long long>(t.createdAt),
      static_cast<long long>(t.updatedAt), t.idempotencyKey.c_str());
  (void)r;
}
}  // namespace detail

// Enqueue a task onto the named queue. Mutates `task` to fill in id/state/times.
inline void enqueue(redisContext* c, Task& task, const std::string& queue,
                    const EnqueueOptions& opts = {}) {
  if (task.type.empty()) throw std::invalid_argument("taskq: task type is empty");
  if (task.id.empty()) task.id = detail::generateId();
  task.queue = queue;
  if (!opts.idempotencyKey.empty()) task.idempotencyKey = opts.idempotencyKey;
  int64_t now = detail::nowMillis();
  task.createdAt = now;
  task.updatedAt = now;

  // Register the queue so tooling can enumerate it even while empty.
  detail::command(c, "SADD %s %s", keys::queues().c_str(), queue.c_str());

  if (opts.scheduled) {
    task.state = State::Scheduled;
    detail::writeTaskHash(c, task);
    detail::command(c, "ZADD %s %lld %s", keys::scheduled(queue).c_str(),
                    static_cast<long long>(detail::toMillis(opts.runAt)), task.id.c_str());
  } else {
    task.state = State::Pending;
    detail::writeTaskHash(c, task);
    detail::command(c, "LPUSH %s %s", keys::pending(queue).c_str(), task.id.c_str());
  }
}

// Pause / resume a queue. A paused queue keeps accepting enqueues but the
// server stops dispatching from it until resumed.
inline void pause(redisContext* c, const std::string& queue) {
  detail::command(c, "SET %s 1", keys::paused(queue).c_str());
}
inline void resume(redisContext* c, const std::string& queue) {
  detail::command(c, "DEL %s", keys::paused(queue).c_str());
}
inline bool isPaused(redisContext* c, const std::string& queue) {
  detail::Reply r = detail::command(c, "EXISTS %s", keys::paused(queue).c_str());
  return r->integer == 1;
}

// ---------------------------------------------------------------------------
// Server configuration
// ---------------------------------------------------------------------------
struct ServerConfig {
  // Redis connection.
  std::string host = "127.0.0.1";
  int port = 6379;

  // queue name -> weight. Higher weight queues are polled more often.
  std::map<std::string, int> queues = {{"default", 1}};

  // How long a worker may hold a task before it's considered dead and the
  // task is recovered.
  std::chrono::milliseconds leaseDuration{30000};

  // How often the dispatch loop wakes to poll queues.
  std::chrono::milliseconds pollInterval{500};

  // Base delay for the exponential retry backoff. The actual delay is
  // jittered within [d/2, d] where d = base * 2^retry.
  std::chrono::milliseconds retryBackoff{1000};

  // Worker thread count. 0 => hardware_concurrency.
  size_t concurrency = 0;

  // How many completed / dead task ids to retain per queue for inspection.
  int historyLimit = 1000;

  // Time-to-live for idempotency markers. 0 => keep forever. A finite TTL
  // bounds the dedup window (a key can run again once its marker expires).
  std::chrono::milliseconds idempotencyTTL{0};

  // How many tasks to keep in flight per worker. The dispatcher leases up to
  // (concurrency * prefetch) tasks before waiting, so workers are never
  // starved between polls. Higher values raise throughput at the cost of more
  // simultaneously-leased tasks.
  int prefetch = 16;
};

// ---------------------------------------------------------------------------
// Server
//
// Owns a dedicated Redis connection for its dispatch loop and hands work to a
// thread pool. Each worker thread opens its own connection (hiredis contexts
// are not thread-safe).
// ---------------------------------------------------------------------------
class Server {
 public:
  explicit Server(ServerConfig config) : config_(std::move(config)) {
    if (config_.concurrency == 0) {
      config_.concurrency = std::max(1u, std::thread::hardware_concurrency());
    }
  }

  ~Server() { stop(); }

  // Run the dispatch loop until stop() is called (from a signal handler or
  // another thread). Blocks the calling thread.
  void run() {
    running_ = true;
    redisContext* ctx = connect();
    ThreadPool pool(config_.concurrency);

    // Expand the weighted queue map into a polling order: a queue with weight
    // 3 appears three times, so it's checked three times per sweep.
    std::vector<std::string> order;
    for (const auto& [name, weight] : config_.queues) {
      detail::command(ctx, "SADD %s %s", keys::queues().c_str(), name.c_str());
      for (int i = 0; i < std::max(1, weight); ++i) order.push_back(name);
    }

    const size_t window = std::max<size_t>(1, config_.concurrency * std::max(1, config_.prefetch));
    int64_t lastHousekeep = 0;

    while (running_) {
      int64_t now = detail::nowMillis();

      // Housekeeping (promote scheduled + recover expired leases) is throttled
      // to once per poll interval instead of running on every dispatch, so it
      // doesn't add two EVALs of overhead to every task under load.
      if (now - lastHousekeep >= config_.pollInterval.count()) {
        for (const auto& q : config_.queues) {
          promoteScheduled(ctx, q.first, now);
          recoverStuck(ctx, q.first, now);
        }
        lastHousekeep = now;
      }

      // Drain: lease tasks (honoring queue weights) until the in-flight window
      // is full or every queue is empty. Batching many dequeues per wake-up,
      // rather than one, is what lets throughput approach the dispatch ceiling.
      bool full = false;
      bool moreWork = true;
      while (moreWork && running_) {
        moreWork = false;
        for (const auto& q : order) {
          if (inFlight_.load() >= window) {
            full = true;
            break;
          }
          std::string id = dequeue(ctx, q, detail::nowMillis());
          if (!id.empty()) {
            moreWork = true;
            inFlight_.fetch_add(1);
            pool.submit([this, id, q] { process(id, q); });
          }
        }
        if (full) break;
      }

      // Sleep until a worker frees a slot (short wait) or, if idle, until the
      // next poll. Completions notify wakeCv_, so we wake promptly under load.
      std::unique_lock<std::mutex> lock(wakeMutex_);
      wakeCv_.wait_for(lock, full ? std::chrono::milliseconds(2) : config_.pollInterval,
                       [this] { return !running_; });
    }

    pool.shutdown();
    redisFree(ctx);
  }

  // Signal the dispatch loop to exit. Safe to call from another thread.
  void stop() {
    running_ = false;
    wakeCv_.notify_all();
  }

 private:
  redisContext* connect() {
    redisContext* c = redisConnect(config_.host.c_str(), config_.port);
    if (c == nullptr || c->err) {
      std::string msg = c ? c->errstr : "allocation failure";
      if (c) redisFree(c);
      throw std::runtime_error("taskq: cannot connect to Redis: " + msg);
    }
    return c;
  }

  void promoteScheduled(redisContext* c, const std::string& q, int64_t now) {
    detail::command(c, "EVAL %s 2 %s %s %lld", lua::kPromoteScheduled,
                    keys::scheduled(q).c_str(), keys::pending(q).c_str(),
                    static_cast<long long>(now));
  }

  void recoverStuck(redisContext* c, const std::string& q, int64_t now) {
    detail::command(c, "EVAL %s 2 %s %s %lld", lua::kRecover, keys::active(q).c_str(),
                    keys::pending(q).c_str(), static_cast<long long>(now));
  }

  std::string dequeue(redisContext* c, const std::string& q, int64_t now) {
    int64_t deadline = now + config_.leaseDuration.count();
    detail::Reply r = detail::command(c, "EVAL %s 3 %s %s %s %lld %lld", lua::kDequeue,
                                      keys::paused(q).c_str(), keys::pending(q).c_str(),
                                      keys::active(q).c_str(),
                                      static_cast<long long>(deadline),
                                      static_cast<long long>(now));
    if (r->type == REDIS_REPLY_STRING) return std::string(r->str, r->len);
    return {};
  }

  // Each worker thread keeps one long-lived Redis connection, reused across
  // every task it handles. Opening a fresh connection per task exhausts the
  // OS's ephemeral ports under load, so the connection is cached in
  // thread-local storage and only reopened after an error.
  redisContext* workerConn() {
    struct Holder {
      redisContext* c = nullptr;
      ~Holder() {
        if (c) redisFree(c);
      }
    };
    thread_local Holder holder;
    if (holder.c && holder.c->err == 0) return holder.c;
    if (holder.c) {
      redisFree(holder.c);
      holder.c = nullptr;
    }
    redisContext* c = redisConnect(config_.host.c_str(), config_.port);
    if (c == nullptr || c->err) {
      if (c) redisFree(c);
      return nullptr;
    }
    holder.c = c;
    return c;
  }

  // Executed on a pool thread: load the task, run its handler, record outcome.
  void process(const std::string& id, const std::string& queue) {
    // Whatever happens, mark the slot free and wake the dispatcher so it can
    // lease the next task immediately.
    struct SlotGuard {
      Server* s;
      ~SlotGuard() {
        s->inFlight_.fetch_sub(1);
        s->wakeCv_.notify_one();
      }
    } slotGuard{this};

    redisContext* c = workerConn();
    if (c == nullptr) {
      std::cerr << "taskq worker: cannot connect to Redis" << std::endl;
      return;  // task keeps its lease and is recovered later
    }

    try {
      Task task = loadTask(c, id);
      task.queue = queue;
      auto it = detail::registry().find(task.type);

      if (it == detail::registry().end()) {
        fail(c, task, "no handler registered for type '" + task.type + "'");
        return;
      }

      // Idempotency: if this key's side effect already ran (marker present),
      // skip the handler and return the original result. This makes a
      // redelivered or duplicate task a no-op.
      bool skip = false;
      if (!task.idempotencyKey.empty()) {
        detail::Reply m =
            detail::command(c, "GET %s", keys::idem(queue, task.idempotencyKey).c_str());
        if (m->type == REDIS_REPLY_STRING) {
          task.result.assign(m->str, m->len);
          skip = true;
        }
      }

      try {
        if (!skip) it->second(task);
        complete(c, task);
      } catch (const std::exception& e) {
        fail(c, task, e.what());
      } catch (...) {
        fail(c, task, "unknown exception");
      }
    } catch (const std::exception& e) {
      // A Redis-level error (thrown by detail::command). The connection is now
      // flagged and workerConn() will reopen it next time; this task keeps its
      // lease and will be recovered.
      std::cerr << "taskq worker: " << e.what() << std::endl;
    }
  }

  Task loadTask(redisContext* c, const std::string& id) {
    Task t;
    t.id = id;
    detail::Reply r = detail::command(c, "HGETALL %s", keys::task(id).c_str());
    if (r->type != REDIS_REPLY_ARRAY) return t;
    for (size_t i = 0; i + 1 < r->elements; i += 2) {
      std::string field(r->element[i]->str, r->element[i]->len);
      std::string value(r->element[i + 1]->str, r->element[i + 1]->len);
      if (field == "type") t.type = value;
      else if (field == "payload") t.payload = value;
      else if (field == "result") t.result = value;
      else if (field == "queue") t.queue = value;
      else if (field == "state") t.state = stateFromString(value);
      else if (field == "maxRetries") t.maxRetries = std::stoi(value);
      else if (field == "retryCount") t.retryCount = std::stoi(value);
      else if (field == "lastError") t.lastError = value;
      else if (field == "createdAt") t.createdAt = std::stoll(value);
      else if (field == "updatedAt") t.updatedAt = std::stoll(value);
      else if (field == "idempotencyKey") t.idempotencyKey = value;
    }
    return t;
  }

  void complete(redisContext* c, const Task& t) {
    std::string marker =
        t.idempotencyKey.empty() ? "" : keys::idem(t.queue, t.idempotencyKey);
    detail::command(c, "EVAL %s 2 %s %s %s %s %lld %d %s %lld", lua::kComplete,
                    keys::active(t.queue).c_str(), keys::completed(t.queue).c_str(),
                    t.id.c_str(), t.result.c_str(),
                    static_cast<long long>(detail::nowMillis()), config_.historyLimit,
                    marker.c_str(), static_cast<long long>(config_.idempotencyTTL.count()));
  }

  void fail(redisContext* c, const Task& t, const std::string& error) {
    int64_t now = detail::nowMillis();
    // Exponential backoff with equal jitter, based on retries already used.
    int64_t backoff = config_.retryBackoff.count() * (1LL << std::min(t.retryCount, 20));
    int64_t retryAt = now + detail::jitter(backoff);
    detail::command(c, "EVAL %s 4 %s %s %s %s %s %s %lld %lld %d", lua::kFail,
                    keys::active(t.queue).c_str(), keys::pending(t.queue).c_str(),
                    keys::scheduled(t.queue).c_str(), keys::dead(t.queue).c_str(),
                    t.id.c_str(), error.c_str(), static_cast<long long>(now),
                    static_cast<long long>(retryAt), config_.historyLimit);
  }

  ServerConfig config_;
  std::atomic<bool> running_{false};
  std::atomic<size_t> inFlight_{0};  // leased tasks not yet finished
  std::mutex wakeMutex_;
  std::condition_variable wakeCv_;
};

// Convenience: build and run a server in one call.
inline void runServer(const ServerConfig& config) {
  Server server(config);
  server.run();
}

}  // namespace taskq

#endif  // TASKQ_HPP
