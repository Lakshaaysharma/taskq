// taskq regression suite (Catch2 v3).
//
// These tests talk to a live Redis on 127.0.0.1:6379 and use an isolated key
// prefix by flushing a dedicated logical DB (SELECT 15). Build with
// ./build_tests.sh, then run ./tests. Tags let you focus: [queue],
// [schedule], [retry], [recovery], [pause], [threadpool].

#include <catch2/catch_test_macros.hpp>

#include "taskq.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace std::chrono_literals;

namespace {

// Connect to a scratch DB so tests never touch real data.
redisContext* testConn() {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  REQUIRE(c != nullptr);
  REQUIRE(c->err == 0);
  redisReply* r = static_cast<redisReply*>(redisCommand(c, "SELECT 15"));
  freeReplyObject(r);
  r = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(r);
  return c;
}

// A server that runs on DB 15 in a background thread.
struct BackgroundServer {
  taskq::Server server;
  std::thread thread;

  explicit BackgroundServer(taskq::ServerConfig cfg) : server(std::move(cfg)) {
    thread = std::thread([this] { server.run(); });
  }
  ~BackgroundServer() {
    server.stop();
    if (thread.joinable()) thread.join();
  }
};

taskq::ServerConfig fastConfig(std::map<std::string, int> queues) {
  taskq::ServerConfig cfg;
  cfg.queues = std::move(queues);
  cfg.pollInterval = 50ms;
  cfg.leaseDuration = 2000ms;
  cfg.retryBackoff = 100ms;
  cfg.concurrency = 4;
  // Point the worker connections at the scratch DB. The Server connects to
  // db 0 by default, so tests that need DB 15 select it via the SELECT below.
  return cfg;
}

std::string hget(redisContext* c, const std::string& key, const std::string& field) {
  redisReply* r =
      static_cast<redisReply*>(redisCommand(c, "HGET %s %s", key.c_str(), field.c_str()));
  std::string out = (r && r->type == REDIS_REPLY_STRING) ? std::string(r->str, r->len) : "";
  if (r) freeReplyObject(r);
  return out;
}

int64_t llen(redisContext* c, const std::string& key) {
  redisReply* r = static_cast<redisReply*>(redisCommand(c, "LLEN %s", key.c_str()));
  int64_t n = (r && r->type == REDIS_REPLY_INTEGER) ? r->integer : -1;
  if (r) freeReplyObject(r);
  return n;
}

}  // namespace

TEST_CASE("thread pool runs all submitted jobs", "[threadpool]") {
  std::atomic<int> counter{0};
  {
    taskq::ThreadPool pool(4);
    for (int i = 0; i < 100; ++i) pool.submit([&counter] { counter.fetch_add(1); });
    pool.shutdown();
  }
  REQUIRE(counter.load() == 100);
}

TEST_CASE("id generation is unique and well-formed", "[queue]") {
  std::string a = taskq::detail::generateId();
  std::string b = taskq::detail::generateId();
  REQUIRE(a.size() == 36);
  REQUIRE(a[14] == '4');  // version nibble
  REQUIRE(a != b);
}

TEST_CASE("enqueue writes hash and pending list", "[queue]") {
  redisContext* c = testConn();
  taskq::Task t{"test:type", "{\"x\":1}", 3};
  taskq::enqueue(c, t, "default");

  REQUIRE(!t.id.empty());
  REQUIRE(t.state == taskq::State::Pending);
  REQUIRE(hget(c, taskq::keys::task(t.id), "type") == "test:type");
  REQUIRE(hget(c, taskq::keys::task(t.id), "payload") == "{\"x\":1}");
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "pending");
  REQUIRE(llen(c, taskq::keys::pending("default")) == 1);

  redisReply* r = static_cast<redisReply*>(
      redisCommand(c, "SISMEMBER %s default", taskq::keys::queues().c_str()));
  REQUIRE(r->integer == 1);
  freeReplyObject(r);
  redisFree(c);
}

TEST_CASE("scheduled enqueue lands on the scheduled zset", "[schedule]") {
  redisContext* c = testConn();
  taskq::Task t{"test:type", "{}", 0};
  taskq::enqueue(c, t, "default", taskq::runIn(1h));

  REQUIRE(t.state == taskq::State::Scheduled);
  REQUIRE(llen(c, taskq::keys::pending("default")) == 0);
  redisReply* r = static_cast<redisReply*>(
      redisCommand(c, "ZCARD %s", taskq::keys::scheduled("default").c_str()));
  REQUIRE(r->integer == 1);
  freeReplyObject(r);
  redisFree(c);
}

TEST_CASE("pause / resume toggles the flag", "[pause]") {
  redisContext* c = testConn();
  REQUIRE(taskq::isPaused(c, "default") == false);
  taskq::pause(c, "default");
  REQUIRE(taskq::isPaused(c, "default") == true);
  taskq::resume(c, "default");
  REQUIRE(taskq::isPaused(c, "default") == false);
  redisFree(c);
}

TEST_CASE("promote scheduled Lua moves due tasks to pending", "[schedule]") {
  redisContext* c = testConn();
  taskq::Task t{"test:type", "{}", 0};
  // Schedule in the past so it is immediately due.
  taskq::enqueue(c, t, "default", taskq::runAt(std::chrono::system_clock::now() - 1min));

  int64_t now = taskq::detail::nowMillis();
  redisReply* r = static_cast<redisReply*>(redisCommand(
      c, "EVAL %s 2 %s %s %lld", taskq::lua::kPromoteScheduled,
      taskq::keys::scheduled("default").c_str(), taskq::keys::pending("default").c_str(),
      static_cast<long long>(now)));
  REQUIRE(r->integer == 1);
  freeReplyObject(r);
  REQUIRE(llen(c, taskq::keys::pending("default")) == 1);
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "pending");
  redisFree(c);
}

TEST_CASE("recover Lua requeues expired leases", "[recovery]") {
  redisContext* c = testConn();
  taskq::Task t{"test:type", "{}", 0};
  taskq::enqueue(c, t, "default");

  // Simulate a lease that expired 10s ago.
  int64_t past = taskq::detail::nowMillis() - 10000;
  redisReply* r = static_cast<redisReply*>(redisCommand(
      c, "ZADD %s %lld %s", taskq::keys::active("default").c_str(),
      static_cast<long long>(past), t.id.c_str()));
  freeReplyObject(r);
  // Also pop it off pending so only the active copy exists.
  r = static_cast<redisReply*>(redisCommand(c, "RPOP %s", taskq::keys::pending("default").c_str()));
  freeReplyObject(r);

  int64_t now = taskq::detail::nowMillis();
  r = static_cast<redisReply*>(redisCommand(
      c, "EVAL %s 2 %s %s %lld", taskq::lua::kRecover, taskq::keys::active("default").c_str(),
      taskq::keys::pending("default").c_str(), static_cast<long long>(now)));
  REQUIRE(r->integer == 1);
  freeReplyObject(r);
  REQUIRE(llen(c, taskq::keys::pending("default")) == 1);
  redisFree(c);
}

TEST_CASE("fail Lua retries until budget is exhausted then buries", "[retry]") {
  redisContext* c = testConn();
  taskq::Task t{"test:type", "{}", 1};  // one retry allowed
  taskq::enqueue(c, t, "default");

  auto runFail = [&]() -> std::string {
    int64_t now = taskq::detail::nowMillis();
    redisReply* r = static_cast<redisReply*>(redisCommand(
        c, "EVAL %s 4 %s %s %s %s %s %s %lld %lld %d", taskq::lua::kFail,
        taskq::keys::active("default").c_str(), taskq::keys::pending("default").c_str(),
        taskq::keys::scheduled("default").c_str(), taskq::keys::dead("default").c_str(),
        t.id.c_str(), "boom", static_cast<long long>(now), static_cast<long long>(now),
        1000));
    std::string out(r->str, r->len);
    freeReplyObject(r);
    return out;
  };

  REQUIRE(runFail() == "retried");
  REQUIRE(hget(c, taskq::keys::task(t.id), "retryCount") == "1");
  REQUIRE(runFail() == "dead");
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "dead");
  REQUIRE(llen(c, taskq::keys::dead("default")) == 1);
  redisFree(c);
}

// --- End-to-end tests exercise a real Server against DB 0 ------------------
// The Server opens its own connections to DB 0, so these use DB 0 and clean up
// after themselves via a unique task type marker.

TEST_CASE("server processes an enqueued task end to end", "[queue][e2e]") {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* fr = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(fr);

  std::atomic<bool> ran{false};
  taskq::registerHandler("e2e:ok", [&ran](taskq::Task& task) {
    ran = true;
    task.result = "{\"done\":true}";
  });

  taskq::Task t{"e2e:ok", "{}", 0};
  taskq::enqueue(c, t, "default");

  BackgroundServer bg(fastConfig({{"default", 1}}));

  for (int i = 0; i < 100 && !ran.load(); ++i) std::this_thread::sleep_for(20ms);
  REQUIRE(ran.load());

  // Give the completion write a moment to land.
  std::this_thread::sleep_for(100ms);
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "completed");
  REQUIRE(hget(c, taskq::keys::task(t.id), "result") == "{\"done\":true}");
  redisFree(c);
}

TEST_CASE("server retries a throwing handler then succeeds", "[retry][e2e]") {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* fr = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(fr);

  std::atomic<int> attempts{0};
  taskq::registerHandler("e2e:flaky", [&attempts](taskq::Task& task) {
    int n = attempts.fetch_add(1) + 1;
    if (n < 2) throw std::runtime_error("transient");
    task.result = "{\"recovered\":true}";
  });

  taskq::Task t{"e2e:flaky", "{}", 3};
  taskq::enqueue(c, t, "default");

  BackgroundServer bg(fastConfig({{"default", 1}}));

  for (int i = 0; i < 200; ++i) {
    if (hget(c, taskq::keys::task(t.id), "state") == "completed") break;
    std::this_thread::sleep_for(20ms);
  }
  REQUIRE(attempts.load() >= 2);
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "completed");
  redisFree(c);
}

TEST_CASE("server buries a permanently failing task as dead", "[retry][e2e]") {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* fr = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(fr);

  taskq::registerHandler("e2e:broken",
                        [](taskq::Task&) { throw std::runtime_error("always fails"); });

  taskq::Task t{"e2e:broken", "{}", 1};  // 1 retry => 2 attempts total
  taskq::enqueue(c, t, "default");

  BackgroundServer bg(fastConfig({{"default", 1}}));

  for (int i = 0; i < 200; ++i) {
    if (hget(c, taskq::keys::task(t.id), "state") == "dead") break;
    std::this_thread::sleep_for(20ms);
  }
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "dead");
  REQUIRE(hget(c, taskq::keys::task(t.id), "lastError") == "always fails");
  REQUIRE(llen(c, taskq::keys::dead("default")) == 1);
  redisFree(c);
}

TEST_CASE("paused queue is not dispatched", "[pause][e2e]") {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* fr = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(fr);

  std::atomic<bool> ran{false};
  taskq::registerHandler("e2e:paused", [&ran](taskq::Task&) { ran = true; });

  taskq::pause(c, "default");
  taskq::Task t{"e2e:paused", "{}", 0};
  taskq::enqueue(c, t, "default");

  BackgroundServer bg(fastConfig({{"default", 1}}));

  std::this_thread::sleep_for(300ms);
  REQUIRE(ran.load() == false);  // still paused

  taskq::resume(c, "default");
  for (int i = 0; i < 100 && !ran.load(); ++i) std::this_thread::sleep_for(20ms);
  REQUIRE(ran.load() == true);
  redisFree(c);
}

TEST_CASE("idempotency key makes duplicate/redelivered tasks no-ops", "[idempotency][e2e]") {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* fr = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(fr);

  std::atomic<int> runs{0};
  taskq::registerHandler("e2e:idem", [&runs](taskq::Task& task) {
    runs.fetch_add(1);
    task.result = "{\"charged\":true}";
  });

  BackgroundServer bg(fastConfig({{"default", 1}}));

  // First task with idempotency key "order-42": the handler runs.
  taskq::Task a{"e2e:idem", "{}", 0};
  taskq::enqueue(c, a, "default", taskq::idempotent("order-42"));
  for (int i = 0; i < 200; ++i) {
    if (hget(c, taskq::keys::task(a.id), "state") == "completed") break;
    std::this_thread::sleep_for(20ms);
  }
  REQUIRE(hget(c, taskq::keys::task(a.id), "state") == "completed");
  REQUIRE(runs.load() == 1);

  // A duplicate/redelivered task with the SAME key must be a no-op that still
  // returns the original result.
  taskq::Task b{"e2e:idem", "{}", 0};
  taskq::enqueue(c, b, "default", taskq::idempotent("order-42"));
  for (int i = 0; i < 200; ++i) {
    if (hget(c, taskq::keys::task(b.id), "state") == "completed") break;
    std::this_thread::sleep_for(20ms);
  }
  REQUIRE(hget(c, taskq::keys::task(b.id), "state") == "completed");
  REQUIRE(runs.load() == 1);  // handler did NOT run a second time
  REQUIRE(hget(c, taskq::keys::task(b.id), "result") == "{\"charged\":true}");
  redisFree(c);
}

TEST_CASE("task abandoned by a crashed worker is reclaimed and completed", "[recovery][e2e]") {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* fr = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  freeReplyObject(fr);

  std::atomic<int> runs{0};
  taskq::registerHandler("e2e:reclaim", [&runs](taskq::Task& task) {
    runs.fetch_add(1);
    task.result = "done";
  });

  taskq::Task t{"e2e:reclaim", "{}", 0};
  taskq::enqueue(c, t, "default");

  // Simulate a worker that leased the task and died before finishing: move it
  // out of pending and into the active set with a lease that already expired.
  redisReply* r =
      static_cast<redisReply*>(redisCommand(c, "RPOP %s", taskq::keys::pending("default").c_str()));
  freeReplyObject(r);
  int64_t expired = taskq::detail::nowMillis() - 5000;
  r = static_cast<redisReply*>(redisCommand(c, "ZADD %s %lld %s",
                                            taskq::keys::active("default").c_str(),
                                            static_cast<long long>(expired), t.id.c_str()));
  freeReplyObject(r);

  // The server's recovery loop should requeue it, after which it runs to completion.
  BackgroundServer bg(fastConfig({{"default", 1}}));
  for (int i = 0; i < 200; ++i) {
    if (hget(c, taskq::keys::task(t.id), "state") == "completed") break;
    std::this_thread::sleep_for(20ms);
  }
  REQUIRE(hget(c, taskq::keys::task(t.id), "state") == "completed");
  REQUIRE(runs.load() == 1);
  redisFree(c);
}
