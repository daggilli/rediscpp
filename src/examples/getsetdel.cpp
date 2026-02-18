#include <print>

#include "rediscpp.hpp"

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  /**
   * Delete an item or items
   *
   * Returns how many items were deleted
   *
   */
  auto del = client.del("fooitem");
  std::println("DEL fooitem {}", del);

  /**
   * del takes an arbitrary number of arguments
   *
   */
  (void)client.del("foo_a", "foo_b", "foo_c", "foo_d", "foo_e");

  /**
   * get returns either a string or std::nullopt to indicate no such key exists, or it is of the wrong type
   *
   * This call will return nullopt as we previously deleted the key
   */
  auto item = client.get("fooitem");
  std::println("fooitem {}", item.value_or("does not exist"));

  /**
   * Determine whether an item exists
   *
   */

  auto exists = client.exists("fooitem");
  std::println("fooitem exists {}", exists);

  /**
   * Set a simple string item
   *
   * Additional flags to SET (expiry, etc.) can be passed after the key and value
   *
   * Returns a Redis reply object. If the 'GET' option was passed, then this will contain a string with the
   * key's previous value, or nil if it did not exist. Otherwise it will be nil or OK depending on interaction
   * with the NX and XX flags.
   *
   */
  auto set = client.set("fooitem", "some value");

  exists = client.exists("fooitem");
  std::println("fooitem exists {}", exists);

  item = client.get("fooitem");
  std::println("fooitem {}", item.value_or("does not exist"));

  // delete again
  del = client.del("fooitem");
  std::println("DEL fooitem {}", del);

  /**
   * set can take additional flags. Here XX means set only if the key already exists. It doesn't, so nothing
   * happens.
   *
   */
  set = client.set("fooitem", "some value", "XX");

  // does not exist
  item = client.get("fooitem");
  std::println("fooitem {}", item.value_or("does not exist (XX flag)"));

  // NX means "set if does not exist"
  set = client.set("fooitem", "another value", "NX");

  // now the key has a value
  item = client.get("fooitem");
  std::println("fooitem {}", item.value_or("does not exist (NX flag)"));

  return 0;
}
