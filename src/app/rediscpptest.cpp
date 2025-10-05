#include <cstdint>
#include <print>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

using namespace std::string_literals;
using namespace std::chrono_literals;
using namespace std::string_view_literals;

struct QueueHandler {
  void operator()(const std::string_view qname, const std::string_view value) {
    std::println("QHAND {} {}", qname, value);
  }
};

struct SubscriptionHandler {
  SubscriptionHandler(const std::string& name_) : name(name_) {}
  void operator()(std::span<std::string_view> elements) {
    std::print("SUB MSG [{}]: ", name);
    for (auto& elem : elements) {
      std::print("{} ", elem);
    }
    std::println("");
  }
  std::string name;
};

int main() {
  RedisCpp::Config cfg("config/redisconfig.json");
  auto client = RedisCpp::Client(cfg);

  // auto pub = client.publish("testch", "a message");

  // QueueHandler qh;
  // auto added = client.addListener("testq", qh);
  // std::println("ADDED {}", added);

  // auto newget = client.get("foox");
  // if (newget.has_value()) {
  //   std::println("ASYNC OPTGET {}", newget.value());
  // }

  SubscriptionHandler sh("thing1"), sh2("thing2");

  client.subscribe("testch", "channel2", "wildcard*", sh);
  client.subscribe("also", sh2);

  auto ix = 0;
  while (ix++ < 40) {
    std::this_thread::sleep_for(1s);
  }

  /* while (true) {
    std::this_thread::sleep_for(1s);
  } */
  return 0;
}
