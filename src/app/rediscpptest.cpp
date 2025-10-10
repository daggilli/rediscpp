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

  QueueHandler qh;
  auto added = client.addListener("testq", qh);
  std::println("ADDED {}", added);

  for (auto i{0u}; i < 5; i++) {
    (void)client.rpush("testq", std::string_view(std::format("QITEM {}", i)));
    // std::this_thread::sleep_for(1ms);
  }

  auto get = client.get("foox");
  if (get.has_value()) {
    std::println("ASYNC OPTGET {}", get.value());
  }

  auto set = client.set("foox", "a foo value", "XX");

  get = client.get("foox");
  if (get.has_value()) {
    std::println("ASYNC OPTGET {}", get.value());
  }

  auto hset = client.hset("foohash", "foo", "bar", "wtf", "nah");

  auto hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("HVEC SZ {} {}", hvec.value().size(), hvec.value());
  }

  auto foo = client.hget("foohash", "foo");
  if (foo.has_value()) {
    std::println("HGET {}", foo.value());
  }

  auto del = client.hdel("foohash", "baz");
  std::println("DET TP {}", del);

  hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("HVEC SZ {} {}", hvec.value().size(), hvec.value());
  }

  auto sadd = client.sadd("fooset", "foo", "bar", "baz");

  auto sism = client.sismember("fooset", "barr");

  std::println("SISM {}", sism);
#if 0
  SubscriptionHandler sh("thing1"), sh2("thing2");

  auto suba = client.subscribe("testch", "channel2", "wildcard*", sh);
  auto subb = client.subscribe("also", sh2);

  std::println("SUBA: {:016X}\nSUBB: {:016X}", suba, subb);
  auto ix = 0;
  while (ix++ < 5) {
    std::this_thread::sleep_for(100ms);
  }

  // (void)client.unsubscribe(suba, "channel2", "wildcard*");
  (void)client.unsubscribe(suba, "channel2");
  // (void)client.unsubscribe(suba);

  ix = 0;
  while (ix++ < 10) {
    std::this_thread::sleep_for(1s);
  }

  client.stop(suba);

  ix = 0;
  while (ix++ < 7) {
    std::this_thread::sleep_for(1s);
  }

  auto subc = client.subscribe("newch", sh2);

  std::println("SUBC: {:016X}", subc);

  ix = 0;
  while (ix++ < 15) {
    std::this_thread::sleep_for(1s);
  }
#endif

  while (true) {
    std::this_thread::sleep_for(1s);
  }
  return 0;
}
