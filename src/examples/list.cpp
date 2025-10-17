#include <format>
#include <print>

#include "rediscppconfig.hpp"
#include "rediscppimpl.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  // tidy up
  (void)client.del("foolist");

  /** push some items onto the left side of queue. The return value is the number of items pushed. The rpush
   * function pushes items onto the right end of the queue.
   */
  auto lpush = client.lpush("foolist", "foo", "bar", "baz", "omg", "wth", "bbq");
  std::println("lpush {}", lpush);

  /**
   * lrange retrieves a range of items from a list given a start and stop index. It returns an optional vector
   * of strings.
   *
   */
  auto lrange = client.lrange("foolist", 0, -1);
  if (lrange.has_value()) {
    for (auto& v : lrange.value()) {
      std::print("{} ", v);
    }
    std::println("");
  }

  /**
   * rpop removes an item from the right end of a list. It returns an optional string. lpop does the same at
   * the left end.
   *
   */
  auto rpop = client.rpop("foolist", 2);
  if (rpop.has_value()) {
    for (auto& i : rpop.value()) {
      std::print("{} ", i);
    }
    std::println("");
  }

  /**
   * llen returns the length of a list or zero if it does not exist. If the key is not a list, it will throw.
   *
   */
  auto llen = client.llen("foolist");
  std::println("llen {}", llen);

  return 0;
}
