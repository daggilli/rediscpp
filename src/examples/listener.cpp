#include <format>
#include <print>
#include <thread>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

using namespace std::chrono_literals;

struct QueueHandler {
  void operator()(const std::string_view qname, const std::string_view value) {
    std::println("QueueHandler message: queue |{}| message |{}|", qname, value);
  }
};

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  QueueHandler qh;
  auto added = client.addListener("testq", qh);
  std::println("ADDED {}", added);

  for (auto i{0u}; i < 5; i++) {
    (void)client.rpush("testq", std::string_view(std::format("queue item {}", i)));
  }

  // pause for 0.1s so we do not exit immediately, to give the queue handler time to print the messages
  std::this_thread::sleep_for(100ms);

  return 0;
}
