// taskq benchmark harness.
//
// Measures two things against a live local Redis:
//   1. Throughput  — pre-load N tasks, then drain them and report jobs/sec.
//   2. Latency     — enqueue at a low rate (below saturation) and report the
//                    enqueue->handler-start latency distribution (p50/p95/p99).
//
// Numbers are machine- and Redis-dependent; treat them as a relative measure.
//
// Build:
//   g++ -std=c++17 -O2 bench.cpp -I. \
//       -I$(brew --prefix hiredis)/include \
//       -L$(brew --prefix hiredis)/lib -lhiredis -lpthread -o bench
// Run:
//   ./bench                 # defaults: 50000 throughput tasks, 5000 latency samples
//   ./bench 100000 8        # N tasks, W workers

#include "taskq.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

using clk = std::chrono::steady_clock;

static int64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch())
      .count();
}

// Shared state for the latency benchmark.
static std::mutex g_mu;
static std::unordered_map<std::string, int64_t> g_enqueuedAt;  // id -> ns
static std::vector<double> g_latenciesMs;
static std::atomic<int> g_done{0};

static void flushdb() {
  redisContext* c = redisConnect("127.0.0.1", 6379);
  redisReply* r = static_cast<redisReply*>(redisCommand(c, "FLUSHDB"));
  if (r) freeReplyObject(r);
  redisFree(c);
}

static double percentile(std::vector<double>& v, double p) {
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  double idx = (p / 100.0) * (v.size() - 1);
  size_t lo = static_cast<size_t>(std::floor(idx));
  size_t hi = static_cast<size_t>(std::ceil(idx));
  if (lo == hi) return v[lo];
  return v[lo] + (v[hi] - v[lo]) * (idx - lo);
}

static taskq::ServerConfig benchConfig(size_t workers) {
  taskq::ServerConfig cfg;
  cfg.queues = {{"default", 1}};
  cfg.concurrency = workers;
  cfg.pollInterval = std::chrono::milliseconds(1);
  cfg.leaseDuration = std::chrono::milliseconds(60000);
  return cfg;
}

static void runThroughput(int n, size_t workers) {
  flushdb();
  g_done = 0;
  taskq::registerHandler("bench:noop", [](taskq::Task&) { g_done.fetch_add(1); });

  redisContext* c = redisConnect("127.0.0.1", 6379);
  for (int i = 0; i < n; ++i) {
    taskq::Task t{"bench:noop", "{}", 0};
    taskq::enqueue(c, t, "default");
  }
  redisFree(c);

  taskq::Server server(benchConfig(workers));
  int64_t start = nowNs();
  std::thread th([&server] { server.run(); });
  while (g_done.load() < n) std::this_thread::sleep_for(std::chrono::milliseconds(2));
  int64_t elapsed = nowNs() - start;
  server.stop();
  th.join();

  double secs = elapsed / 1e9;
  std::printf("  tasks           : %d\n", n);
  std::printf("  workers         : %zu\n", workers);
  std::printf("  elapsed         : %.3f s\n", secs);
  std::printf("  throughput      : %.0f jobs/sec\n", n / secs);
}

static void runLatency(int samples, size_t workers) {
  flushdb();
  {
    std::lock_guard<std::mutex> lk(g_mu);
    g_enqueuedAt.clear();
    g_latenciesMs.clear();
  }
  taskq::registerHandler("bench:lat", [](taskq::Task& t) {
    int64_t end = nowNs();
    std::lock_guard<std::mutex> lk(g_mu);
    auto it = g_enqueuedAt.find(t.id);
    if (it != g_enqueuedAt.end()) g_latenciesMs.push_back((end - it->second) / 1e6);
  });

  taskq::Server server(benchConfig(workers));
  std::thread th([&server] { server.run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));  // let workers settle

  redisContext* c = redisConnect("127.0.0.1", 6379);
  for (int i = 0; i < samples; ++i) {
    taskq::Task t{"bench:lat", "{}", 0};
    t.id = taskq::detail::generateId();  // set id up front so we can time it
    {
      std::lock_guard<std::mutex> lk(g_mu);
      g_enqueuedAt[t.id] = nowNs();
    }
    taskq::enqueue(c, t, "default");
    std::this_thread::sleep_for(std::chrono::milliseconds(1));  // stay below saturation
  }
  redisFree(c);

  for (int i = 0; i < 500; ++i) {
    {
      std::lock_guard<std::mutex> lk(g_mu);
      if (static_cast<int>(g_latenciesMs.size()) >= samples) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  server.stop();
  th.join();

  std::vector<double> v;
  {
    std::lock_guard<std::mutex> lk(g_mu);
    v = g_latenciesMs;
  }
  double sum = 0;
  for (double x : v) sum += x;
  std::printf("  samples         : %zu\n", v.size());
  std::printf("  mean            : %.2f ms\n", v.empty() ? 0.0 : sum / v.size());
  std::printf("  p50             : %.2f ms\n", percentile(v, 50));
  std::printf("  p95             : %.2f ms\n", percentile(v, 95));
  std::printf("  p99             : %.2f ms\n", percentile(v, 99));
}

int main(int argc, char** argv) {
  int n = argc > 1 ? std::atoi(argv[1]) : 50000;
  size_t workers = argc > 2 ? static_cast<size_t>(std::atoi(argv[2]))
                            : std::max(2u, std::thread::hardware_concurrency());
  int latSamples = 5000;

  std::printf("taskq benchmark (Redis @ 127.0.0.1:6379)\n\n");
  std::printf("[throughput]\n");
  runThroughput(n, workers);
  std::printf("\n[latency] (low-rate, enqueue->handler-start)\n");
  runLatency(latSamples, workers);
  return 0;
}
