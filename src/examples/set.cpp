#include <print>

#include "rediscpp.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  // tidy up
  (void)client.del("fooset");

  /**
   * Add items to set. The return values is the number of items inserted.
   *
   */
  auto sadd = client.sadd("fooset", "foo", "bar", "baz");
  std::println("sadd count: {}", sadd);

  /**
   * Find if a set contains a value. If the key does not refer to a set
   * type, the function throws.
   *
   */
  auto sism = client.sismember("fooset", "bar");
  std::println("sism bar: {}", sism);

  /**
   * Get all members of a set. The return value is an optional vector of strings or nullopt if the set is
   * empty.  If the key does not refer to a set type, the function throws.
   *
   *
   */
  auto mems = client.smembers("fooset");
  if (mems.has_value()) {
    std::println("smembers fooset: {}", mems.value());
  }

  /**
   * Remove elenents from a set. The return value is how many elements were removed.
   *
   */
  auto srem = client.srem("fooset", "bar", "baz");
  std::println("srem bar, baz: {}", srem);

  // check to see that members have been removed
  mems = client.smembers("fooset");
  if (mems.has_value()) {
    std::println("smembers fooset: {}", mems.value());
  }

  return 0;
}
