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

int main(int argc, char* argv[]) {
  RedisCpp::Config cfg("config/redisconfig.json");
  auto client = RedisCpp::Client(cfg);

#if 1
  // auto scriptHash = client.loadScript("fooscript", "return 'Lua script is foomatic'");
  auto scriptHash = client.loadScriptFromFile("fooscript", "scripts/setkey.lua");
  std::string sha;
  std::println("SCRPT SHA {}", scriptHash.value_or("BAD LOAD"));
  if (scriptHash.has_value()) {
    sha = scriptHash.value();
    // auto result = client.evaluateBySha(sha, 1, "wtfkey", "a value");
    auto result = client.evaluate("fooscript", 1, "wtfkey", "another value");
    // std::println("RES {} {}", result->type, result->str);
    std::println("RES {}", result->type);
  }
  auto mems = client.smembers("fooset");
  if (mems.has_value()) {
    std::println("SMEMS: {}", mems.value());
  }

  std::println("APP SCR EX {}", client.scriptExists("fooscript"));

  auto exists = client.exists("fooset", "foohash");
  std::println("EXISTS {}", exists);

  auto lpop = client.lpop("nlist", 2);
  if (lpop.has_value()) {
    std::println("LPOP {}", lpop.value());
  }

  // (void)client.lpush("nlist", "foo", "bar", "baz", "zomg", "wtf", "bbq");

  auto lran = client.lrange("nlist", 0, -1);
  if (lran.has_value()) {
    for (auto& v : lran.value()) {
      std::print("{} ", v);
    }
    std::println("");
  }

  auto abc = std::string_view("abc");
  std::println("ABC SZ {} {}", abc.size(), (int)*(abc.data() + abc.size()));

  auto abcs = std::string("GET %s");
  auto abcsv = std::string_view(abcs.data(), abcs.length());
  std::println("ABCsv SZ {} {}", abcsv.size(), (int)*(abcsv.data() + abcsv.size()));

  (void)client.command("GET %s", "foox");
  (void)client.command(abcs, "foox");
  (void)client.command(abcsv, "foox");
  (void)client.command(std::string("GET %s"), "foox");
  auto rep = client.command(std::move(abcs), "foox");
  std::println("REP {}", rep->str);
#endif
#if 0
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
#endif

#if 0
  SubscriptionHandler sh("thing1"), sh2("thing2");

  auto suba = client.subscribe("testch", "channel2", "wildcard*", sh);
  // auto subb = client.subscribe("also", sh2);

  // std::println("SUBA: {:016X}\nSUBB: {:016X}", suba, subb);
  // auto ix = 0;
  // while (ix++ < 10) {
  //   std::this_thread::sleep_for(100ms);
  // }

  auto ix{0u};
  if (argc > 1) {
    while (ix++ < 10) {
      std::this_thread::sleep_for(100ms);
    }
  }

  // (void)client.unsubscribe(suba, "channel2", "wildcard*");
  (void)client.unsubscribe(suba, "channel2");
  // (void)client.unsubscribe(suba);
  // std::this_thread::sleep_for(100ms);

  auto added = client.subscribe(suba, "bingchilling", "foobar*");
  std::println("ADDED {}", added);
#endif

#if 0
  ix = 0;
  while (ix++ < 3) {
    std::this_thread::sleep_for(100ms);
  }

  client.stop(suba);

  ix = 0;
  while (ix++ < 4) {
    std::this_thread::sleep_for(100ms);
  }

  auto subc = client.subscribe("newch", sh2);

  std::println("SUBC: {:016X}", subc);

  ix = 0;
  while (ix++ < 6) {
    std::this_thread::sleep_for(100ms);
  }

  auto added = client.subscribe(suba, "bingchilling", "foobar*");
  std::println("ADDED {}", added);
#endif

  while (true) {
    std::this_thread::sleep_for(1s);
  }
  return 0;
}
