#include <format>
#include <print>

#include "rediscppconfig.hpp"
#include "rediscppimpl.hpp"

int main() {
  /**
   * A Client needs a Config
   *
   */
  RedisCpp::Config config("localhost", 6379);
  RedisCpp::Client client(config);

  /**
   * A Client cannot be copied
   *
   */
  // auto otherclient = std::move(client);
  // ^ does not compile

  /**
   * A Client cannot be moved
   *
   */
  // auto otherclient = std::move(client);
  // ^ does not compile

  /**
   * When a Client goes out of scope it is destroyed.
   *
   */
  return 0;
}