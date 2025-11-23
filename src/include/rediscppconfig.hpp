#pragma once
#include <filesystem>
#include <format>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <toml++/toml.hpp>
#include <utility>

namespace RedisCpp {
  namespace fs = std::filesystem;

  constexpr const char *const DEFAULT_HOST = "localhost";
  constexpr int DEFAULT_PORT = 6379;

  /**
   * @brief A class to handle Redis configuration options
   *
   * @details The configuratiom object has the folling fields:
   * * `hostname` The Redis server host
   * * `port` The Redis server port
   * * `db` The database to be used (default 0)
   * * `useAuth` Whether basic (AUTH) authentication should be used (default false)
   * * `username` The username to be used if AUTH authentication is required
   * * `password` The password to be used if AUTH authentication is required
   * * `useResp3` Whether the RESP3 reply format should be used (default true)
   */
  class Config {
    friend class Client;

   public:
    explicit Config(const std::string &hostname_, int port_, int db_ = 0,
                    std::optional<std::string> un_ = std::nullopt,
                    std::optional<std::string> pw_ = std::nullopt, bool ur3_ = true)
        : hostname{hostname_}, port{port_}, db{db_}, username(un_), password(pw_), useResp3(ur3_) {
      useAuth = username.has_value() && password.has_value();
    };
    Config(const Config &config) = default;
    Config(Config &&config) = default;
    /**
     * @brief Construct a new Config object from a file containing a JSON object
     *
     * @param configFilePath The path to the configuration file
     *
     * @details The JSON configuration object has the following shape (note fields are lowercase):
     * ```json
     * {
          "hostname": string,
          "port": int,
          "db": int,
          "useauth": boolean,
          "username": string,
          "password": string,
          "useresp3": boolean
        }
     * ```
     * All fields are optional. If not present, a field will take the default value.
     */
    explicit Config(const fs::path &configFilePath)
        : db(std::nullopt), useAuth{false}, username(std::nullopt), password(std::nullopt), useResp3{true} {
      auto filepath = fs::weakly_canonical(fs::absolute(configFilePath));
      if (!fs::exists(filepath))
        throw std::runtime_error(std::format("Configuration file not found at: {}", filepath.string()));

      toml::table config;

      try {
        config = toml::parse_file(configFilePath.string());
      } catch (const toml::parse_error &err) {
        throw std::runtime_error(
            std::format("Failed to parse configuration file {}:\n{}", filepath.string(), err.description()));
      }

      auto redis = config["redis"];

      if (!redis.is_table()) {
        throw std::runtime_error(std::format("Missing [redis] section in {}", filepath.string()));
      }

      hostname = redis["hostname"].value_or(DEFAULT_HOST);
      port = redis["port"].value_or(DEFAULT_PORT);
      db = redis["db"].value<int>();
      if (db.has_value()) {
        db = std::clamp(db.value(), 0, 15);
      }
      useResp3 = redis["useresp3"].value_or(true);
      auto auth = redis["useauth"].value_or(false);
      std::optional<std::string> un = redis["username"].value<std::string>();
      std::optional<std::string> pw = redis["password"].value<std::string>();

      if (auth && pw.has_value()) {
        useAuth = true;
        if (un.has_value()) username = un;
        password = pw;
      }
    }

    /**
     * @brief Copy assignment
     *
     * @param config The Config to be copied
     *
     * @return Config& A copy
     *
     */
    Config &operator=(const Config &config) {
      if (this != &config) {
        hostname = config.hostname;
        port = config.port;
        db = config.port;
        useAuth = config.useAuth;
        username = config.username;
        password = config.password;
        useResp3 = config.useResp3;
      }
      return *this;
    }

    /**
     * @brief Move assignment
     *
     * @param config The Config to be moved
     *
     * @return Config& A moved copy
     *
     */
    Config &operator=(Config &&config) {
      if (this != &config) {
        hostname = std::move(config.hostname);
        port = config.port;
        db = config.db;
        useAuth = config.useAuth;
        username = std::move(config.username);
        password = std::move(config.password);
        useResp3 = config.useResp3;
      }
      return *this;
    }

   private:
    std::string hostname;
    int port;
    std::optional<int> db;
    bool useAuth;
    std::optional<std::string> username;
    std::optional<std::string> password;
    bool useResp3;

    friend std::string text(Config const &cfg) {
      return std::format(
          "hostname: {}\n    port: {}\n      db: {}\n useAuth: {}\nusername: {}\npassword: {}\nuseResp3: {}",
          cfg.hostname, cfg.port, cfg.db.value_or(0), cfg.useAuth, cfg.username.value_or("nil"),
          cfg.password.value_or("nil"), cfg.useResp3);
    }

    friend struct std::formatter<Config, char>;
  };
};  // namespace RedisCpp

template <>
struct std::formatter<RedisCpp::Config, char> {
  constexpr auto parse(std::format_parse_context &pc) { return pc.begin(); }

  template <typename Ctx>
  auto format(RedisCpp::Config const &cfg, Ctx &ctx) const {
    return std::format_to(ctx.out(), "{}", text(cfg));
  }
};
