#include <format>
#include <print>

#include "rediscppconfig.hpp"

int main() {
  /**
   * Explicit constructor only needs hostname and port; all other parameters are optional
   *
   */
  RedisCpp::Config cfg_hostport("localhost", 6379);
  std::println("{}", cfg_hostport);

  /**
   * The db parameter sets the database number we will use
   *
   */
  RedisCpp::Config cfg_db("localhost", 6379, 6);
  std::println("\n{}", cfg_db);

  /**
   * Configs can be copied
   *
   */
  auto cfg_copy{cfg_db};
  std::println("\n{}", cfg_copy);

  /**
   * Configs can be moved
   *
   */
  auto cfg_move{std::move(cfg_copy)};
  std::println("\n{}", cfg_move);

  /**
   * Configs can be assigned
   *
   */
  auto cfg_eq = cfg_move;
  std::println("\n{}", cfg_move);

  /**
   * A Config can be loaded from a file containing TOML
   *
   */
  RedisCpp::Config cfg_file("config/config_example.toml");
  std::println("\nFrom TOML file:\n{}", cfg_file);

  return 0;
}
