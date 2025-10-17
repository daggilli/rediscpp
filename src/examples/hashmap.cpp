#include <format>
#include <print>

#include "rediscppconfig.hpp"
#include "rediscppimpl.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  // tidy up
  (void)client.del("foohash");

  /**
   * hset takes a key and a list of key/value pairs. It returns the number of ietms set
   *
   */
  auto hset = client.hset("foohash", "foo", "bar", "baz", "bop");
  std::println("hset count: {}", hset);

  /**
   * hgetall gets all the key/value pairs for a given hash. It returns an optional vector of pairs
   * of strings. If the hash does not exist them std::nullopt is returned.
   *
   */
  auto hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("hvec size: {}, items: {}", hvec.value().size(), hvec.value());
  }

  /**
   * hget queries a hash by item key. If the items is found it is returned as a string, otherwise std::nullopt
   * is returned.
   *
   */
  auto hget = client.hget("foohash", "foo");
  if (hget.has_value()) {
    std::println("hget foohash::foo value: {}", hget.value());
  }

  /**
   * hdel removes items based on their keys keyu. It returnd the number of items deleted.
   *
   */
  auto hdel = client.hdel("foohash", "baz");
  std::println("hdel, count {}", hdel);

  // check to see key 'baz' has been deleted
  hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("hvec size: {}, items: {}", hvec.value().size(), hvec.value());
  }

  // add some more items
  hset = client.hset("foohash", "omg", "bbq", "lol", "wth", "foo", "any");
  std::println("hset count: {}", hset);

  // check insertion. The entry for 'foo' has changed.
  hvec = client.hgetall("foohash");
  if (hvec.has_value()) {
    std::println("hvec size: {}, items: {}", hvec.value().size(), hvec.value());
  }

  /**
   * hmget retrieves a list of items by key. It returns a vector of pairs of string and optional string. If
   * no item is found with a given key then the second item in the pair will be std::nullopt.
   *
   */
  auto hmget = client.hmget("foohash", "foo", "lol", "bbq");
  std::println("hmget count {}", hmget.size());
  for (auto& h : hmget) {
    std::println(" {} -> {}", h.first, h.second.value_or("nil"));
  }

  return 0;
}
