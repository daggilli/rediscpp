#include <print>
#include <thread>

#include "rediscpp.hpp"

using namespace std::chrono_literals;

/**
 * A class (callable object) to act as a listener on the queue. Any callable with an appropriate signature can
 * be uses as a listener: a class or struct such as this one, a lambda, a free function or a class memeber
 * function.
 *
 */
struct QueueHandler {
  void operator()(const std::string_view qname, const std::string_view value) {
    std::println("QueueHandler message: queue |{}| message |{}|", qname, value);
  }
};

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  // create the listener and bind it a queue (list)
  QueueHandler qh;
  auto added = client.addListener("testq", qh);
  std::println("ADDED {}", added);

  /** push some data onto the list to simulate another process producing data. The listener's handler will be
   * called with the messages.
   *
   */
  for (auto i{0u}; i < 5; i++) {
    (void)client.rpush("testq", std::string_view(std::format("queue item {}", i)));
  }

  // pause for 0.1s so we do not exit immediately, to give the queue handler time to print the messages
  std::this_thread::sleep_for(100ms);

  return 0;
}
