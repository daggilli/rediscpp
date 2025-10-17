#include <format>
#include <print>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  (void)client.del("foohash");

  auto hset = client.hset("foohash", "foo", "bar", "baz", "bop");
  std::println("hset count: {}", hset);

  auto hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("hvec size: {}, items: {}", hvec.value().size(), hvec.value());
  }

  auto hget = client.hget("foohash", "foo");
  if (hget.has_value()) {
    std::println("hget foohash::foo value: {}", hget.value());
  }

  auto hdel = client.hdel("foohash", "baz");
  std::println("hdel, count {}", hdel);

  hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("hvec size: {}, items: {}", hvec.value().size(), hvec.value());
  }

  hset = client.hset("foohash", "omg", "bbq", "lol", "wth", "foo", "any");
  std::println("hset count: {}", hset);

  hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("hvec size: {}, items: {}", hvec.value().size(), hvec.value());
  }

  auto hmget = client.hmget("foohash", "foo", "lol", "bbq");
  std::println("hmget count {}", hmget.size());
  for (auto& h : hmget) {
    std::println(" {} -> {}", h.first, h.second.value_or("nil"));
  }
  return 0;
}
