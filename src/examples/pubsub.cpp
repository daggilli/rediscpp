#include <format>
#include <print>
#include <ranges>
#include <shared_mutex>
#include <thread>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

using namespace std::chrono_literals;

struct SubscriptionHandler {
  SubscriptionHandler(const std::string &name_) : name(name_) {}
  void operator()(std::span<std::string_view> elements) {
    std::print("Subscriber message [{}]: --|", name);
    std::string sep{""};
    for (auto &elem : elements) {
      std::print("{}{}", sep, elem);
      sep = " ";
    }
    std::println("|--");
  }
  std::string name;
};

int main() {
  RedisCpp::Config config("localhost", 6379, 6);
  RedisCpp::Client client(config);

  RedisCpp::Client consumerClient(config);

  SubscriptionHandler handlera("thing1"), handlerb("thing2");

  auto suba = client.subscribe("testchan", "secondchan", "sharedchan", "wildcard*", handlera);
  auto subb = client.subscribe("sharedchan", "also", handlerb);

  std::println("suba: {:016X}\nsuba: {:016X}", suba, subb);

  std::this_thread::sleep_for(25ms);

  auto pub = client.publish("testchan", "a message");
  std::println("Num subscribers: {}", pub);

  pub = client.publish("sharedchan", "a message to both");
  std::println("Num subscribers: {}", pub);

  pub = client.publish("wildcard1", "a pattern message");
  std::println("Num subscribers: {}", pub);

  auto added = client.subscribe(suba, "newchan", "foochan*");
  std::println("added {}", added);

  (void)client.unsubscribe(suba, "testchan");
  std::this_thread::sleep_for(25ms);

  pub = client.publish("testchan", "a message");
  std::println("Num subscribers: {}", pub);

  pub = client.publish("secondchan", "a message 1");
  pub = client.publish("secondchan", "a message 2");
  pub = client.publish("secondchan", "a message 3");

  client.stop(subb);
  pub = client.publish("also", "message after stop");
  std::println("Num subscribers: {}", pub);

  std::println("Exit in 1.5s");
  std::this_thread::sleep_for(1500ms);

  return 0;
}
