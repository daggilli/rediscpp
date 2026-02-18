#include <print>
#include <string_view>

#include "rediscpp.hpp"

constexpr std::string_view script = "return redis.call('EXISTS', KEYS[1])";
constexpr std::string_view key = "fooitem";

int main() {
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  // make sure a value exists
  (void)client.set(key, "some value");

  /**
   * Load a script into the cache from a string. The name can be used to refer to it later, or the SHA hash.
   *
   */
  auto maybeScriptHash = client.loadScript("exists", script);
  auto scriptHash = maybeScriptHash.value();
  std::println("SHA {}", scriptHash);

  /**
   * Run the script based on its name. The second argument is the number of keys in the list of parameters
   * following, which are passed to the script in the KEYS array; the remainder are arguments which are passed
   * in the ARGV array.
   *
   */
  auto result = client.evaluate("exists", 1, key);

  std::println("'{}' exists: {}", key, result->integer != 0);

  /**
   * Load a script from a file containing a Lua program. As before, the name can be used to refer to it later,
   * or the SHA hash.
   *
   */
  maybeScriptHash = client.loadScriptFromFile("setkey", "scripts/setkey.lua");
  scriptHash = maybeScriptHash.value();

  std::println("SHA {}", scriptHash);

  /**
   * Evaluate a script based on its SHA hash. The remaining arguments are identical to evaluate().
   *
   */
  (void)client.evaluateBySha(scriptHash, 1, key, "another value");

  // check that the item has the new value
  auto item = client.get(key);
  std::println("'{}': {}", key, item.value_or("does not exist"));

  // confirn the script is in the client cache
  std::println("{} exists: {}", "setkey", client.scriptExists("setkey"));

  // destroy all cached scripts (caution!)
  (void)client.flushScripts();

  // confirn the script is no longer in the client cache
  std::println("{} exists: {}", "setkey", client.scriptExists("setkey"));

  /**
   * Its SHA hash has disappeared from Redis too. Note this function can take multiple hashes and returns an
   * array of bools corresponding to each hash
   *
   */
  auto existsReply = client.scriptsExistBySha(scriptHash);
  std::println("{} exists: {}", "setkey", existsReply);

  return 0;
}
