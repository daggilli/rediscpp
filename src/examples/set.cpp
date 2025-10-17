#include <format>
#include <print>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  (void)client.del("fooset");

  auto sadd = client.sadd("fooset", "foo", "bar", "baz");
  std::println("sadd count: {}", sadd);

  auto sism = client.sismember("fooset", "bar");
  std::println("sism bar: {}", sism);

  auto mems = client.smembers("fooset");
  if (mems.has_value()) {
    std::println("smembers fooset: {}", mems.value());
  }

  auto srem = client.srem("fooset", "bar", "baz");
  std::println("srem bar, baz: {}", srem);

  mems = client.smembers("fooset");
  if (mems.has_value()) {
    std::println("smembers fooset: {}", mems.value());
  }

  return 0;
}
