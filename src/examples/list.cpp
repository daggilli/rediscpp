#include <format>
#include <print>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  (void)client.del("foolist");

  auto lpush = client.lpush("foolist", "foo", "bar", "baz", "omg", "wth", "bbq");
  std::println("lpush {}", lpush);

  auto lrange = client.lrange("foolist", 0, -1);
  if (lrange.has_value()) {
    for (auto& v : lrange.value()) {
      std::print("{} ", v);
    }
    std::println("");
  }

  auto rpop = client.rpop("foolist", 2);
  if (rpop.has_value()) {
    for (auto& i : rpop.value()) {
      std::print("{} ", i);
    }
    std::println("");
  }

  auto llen = client.llen("foolist");
  std::println("llen {}", llen);

  return 0;
}
