// A minimal producer + worker demonstration of taskq.
//
// Build:
//   g++ -std=c++17 example.cpp -I. \
//       -I$(brew --prefix hiredis)/include \
//       -I$(brew --prefix nlohmann-json)/include \
//       -L$(brew --prefix hiredis)/lib -lhiredis -lpthread -o example
//
// Run (with Redis listening on 127.0.0.1:6379):
//   ./example

#include "taskq.hpp"

#include <nlohmann/json.hpp>

#include <csignal>
#include <iostream>

using nlohmann::json;

// Each task type is identified by a string key shared by producer and worker.
const std::string TypeEmailDelivery = "email:deliver";
const std::string TypeImageResize = "image:resize";

// ---- Producer helpers -----------------------------------------------------

taskq::Task NewEmailTask(int userId, const std::string& templateId) {
  json j = {{"userId", userId}, {"templateId", templateId}};
  // The third argument is maxRetries.
  return taskq::Task{TypeEmailDelivery, j.dump(), 5};
}

taskq::Task NewImageResizeTask(const std::string& path, int width) {
  json j = {{"path", path}, {"width", width}};
  return taskq::Task{TypeImageResize, j.dump(), 3};
}

// ---- Handlers -------------------------------------------------------------

void HandleEmail(taskq::Task& task) {
  json p = json::parse(task.payload);
  std::cout << "[email] sending template " << p["templateId"].get<std::string>()
            << " to user " << p["userId"].get<int>() << "\n";
  // ... actually send the email ...
  json r = {{"sent", true}};
  task.result = r.dump();
}

void HandleImageResize(taskq::Task& task) {
  json p = json::parse(task.payload);
  std::cout << "[image] resizing " << p["path"].get<std::string>() << " to width "
            << p["width"].get<int>() << "\n";
  json r = {{"ok", true}};
  task.result = r.dump();
}

// ---- Graceful shutdown ----------------------------------------------------

taskq::Server* gServer = nullptr;
void onSignal(int) {
  if (gServer) gServer->stop();
}

int main() {
  // 1. Register handlers before starting the server.
  taskq::registerHandler(TypeEmailDelivery, &HandleEmail);
  taskq::registerHandler(TypeImageResize, &HandleImageResize);

  // 2. Open a connection for enqueuing.
  redisContext* c = redisConnect("127.0.0.1", 6379);
  if (c == nullptr || c->err) {
    std::cerr << "Failed to connect to Redis\n";
    return 1;
  }

  // 3. Enqueue some work.
  auto email = NewEmailTask(666, "welcome");
  taskq::enqueue(c, email, "default");

  auto urgent = NewEmailTask(607, "password-reset");
  taskq::enqueue(c, urgent, "high");

  auto resize = NewImageResizeTask("/tmp/avatar.png", 128);
  taskq::enqueue(c, resize, "low", taskq::runIn(std::chrono::seconds(5)));  // scheduled

  std::cout << "Enqueued 3 tasks. Starting workers (Ctrl-C to stop)...\n";
  redisFree(c);

  // 4. Configure and run the worker server.
  taskq::ServerConfig cfg;
  cfg.queues = {{"low", 1}, {"default", 2}, {"high", 4}};  // weighted priority
  cfg.concurrency = 4;

  taskq::Server server(cfg);
  gServer = &server;
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  server.run();

  std::cout << "Shut down cleanly.\n";
  return 0;
}
