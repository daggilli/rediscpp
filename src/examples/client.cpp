#include "rediscpp.hpp"

int main() {
  /**
   * A default constructed client will use a default Config
   *
   */
  RedisCpp::Client defclient;

  /**
   * A Client needs a Config if not using default host and port
   *
   */
  RedisCpp::Config config("127.0.0.1", 6380);
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