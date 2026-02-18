#include <chrono>
#include <cstdint>
#include <print>
#include <random>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include "rediscpp.hpp"

using namespace std::chrono_literals;

template <typename IntegralType>
concept Integral = std::is_integral<IntegralType>::value;

// a simple random number generator class
template <Integral IntType>
class RandomIntGenerator {
 public:
  RandomIntGenerator() = default;
  RandomIntGenerator(IntType lower_, IntType upper_) : lower{lower_}, upper{upper_} {}
  IntType operator()() { return std::uniform_int_distribution<IntType>{lower, upper}(engine); }
  IntType operator()(const IntType low, const IntType upp) {
    return std::uniform_int_distribution<IntType>{low, upp}(engine);
  }

 private:
  IntType lower{std::numeric_limits<IntType>::min()};
  IntType upper{std::numeric_limits<IntType>::max()};
  std::mt19937 engine{std::random_device{}()};
};

// base class callable object to act as a subscription listener
class Consumer {
 public:
  Consumer(RedisCpp::Client &cli_, const std::string &nm_, const std::string &qn_)
      : client(cli_), name{nm_}, queueName{qn_} {}
  virtual void operator()(std::span<std::string_view> elements) {
    auto message = name + ":" + (elements | std::views::join_with(':') | std::ranges::to<std::string>());
    std::println("RECEIVED: {}", message);
  }

 protected:
  RedisCpp::Client &client;
  std::string name;
  std::string queueName;
};

/** A subscription listener subclass that holds a mutex so two such objects can share a client connection. It
 * takes subscription messages and pushes them onto a queue. Access to the queue is locked via the mutex.
 */
class SharedConsumer : public Consumer {
 public:
  SharedConsumer(RedisCpp::Client &cli_, const std::string &nm_, const std::string &qn_,
                 std::shared_mutex &mx_)
      : Consumer(cli_, nm_, qn_), mx{mx_} {}
  void operator()(std::span<std::string_view> elements) override {
    if (elements[0] == "subscribe" || elements[0] == "psubscribe") return;
    auto msg = name + ":" + (elements | std::views::join_with(':') | std::ranges::to<std::string>());
    auto lock = std::unique_lock(mx);
    (void)client.rpush(queueName, msg);
  }

 private:
  std::shared_mutex &mx;
};

// a listener for a queue using a list and blocking pop (see listener.cpp)
struct QueueHandler {
  void operator()(const std::string_view qname, const std::string_view value) {
    std::println("QueueHandler message: queue |{}| message |{}|", qname, value);
  }
};

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  (void)client.del("pubsubq");

  RedisCpp::Client consumerClient(config);

  // if two queue handlers are accessing the same Client, they need to synchronise access with a mutex
  // or a similar concurrency mechanism. Redis is single-threaded, so one Client per handler also works,
  // at the expense of using more resources
  std::shared_mutex mx;

  // create callable objects as targets for subscription
  SharedConsumer consumera(consumerClient, "Consumer A", "pubsubq", mx);
  SharedConsumer consumerb(consumerClient, "Consumer B", "pubsubq", mx);

  (void)client.subscribe("testchan", "secondchan", "sharedchan", "wildcard*", consumera);
  (void)client.subscribe("sharedchan", "also", consumerb);

  std::this_thread::sleep_for(25ms);

  auto channels = std::vector<std::string_view>{"testchan", "secondchan", "sharedchan", "wildcardx", "also"};
  auto ran = RandomIntGenerator<uint16_t>{0, static_cast<uint16_t>(channels.size() - 1)};

  // this client acts as a consumer to the subscription listeners producers
  RedisCpp::Client unifiedQueueClient(config);
  QueueHandler handler;

  unifiedQueueClient.addListener("pubsubq", handler);

  // simulate a data source publishing to a channel at random intervals
  auto publish = [&]() {
    RandomIntGenerator<uint16_t> ranDelay(50, 200);
    auto delay = std::chrono::milliseconds{ranDelay()};

    for (auto i{0u}; i < 40; i++) {
      (void)client.publish(channels[ran()], std::format("message {}", i + 1));
      std::this_thread::sleep_for(delay);
    }
  };

  auto pubThread = std::thread(publish);

  pubThread.join();

  std::println("Exit in 1.5s");

  std::this_thread::sleep_for(1500ms);

  return 0;
}
