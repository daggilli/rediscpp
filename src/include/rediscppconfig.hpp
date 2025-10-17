#pragma once

#include <json/reader.h>
#include <json/value.h>

#include <filesystem>
#include <format>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>

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
      auto fsize = fs::file_size(filepath);

      std::unique_ptr<uint8_t[]> inbuffer;
      {
        std::ifstream infile;
        infile.exceptions(std::ifstream::failbit | std::ifstream::badbit);
        try {
          infile.open(filepath.c_str(), std::ios::in | std::ifstream::binary);
        } catch (const std::ifstream::failure &e) {
          throw std::runtime_error(std::format("Can't open input file {}: {} ({}: {})", filepath.string(),
                                               e.what(), e.code().value(), e.code().message()));
        }
        try {
          inbuffer = std::make_unique_for_overwrite<uint8_t[]>(fsize);
        } catch (const std::bad_alloc &e) {
          throw;
        }
        auto buf = std::bit_cast<char *>(inbuffer.get());
        infile.read(buf, fsize);

        JSONCPP_STRING err;
        Json::Value root;
        Json::CharReaderBuilder builder;
        const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        if (!reader->parse(buf, buf + fsize, &root, &err)) {
          throw std::runtime_error(std::format("Config file parse error: {}", err));
        }

        hostname = root.isMember("hostname") ? root["hostname"].asString() : DEFAULT_HOST;
        port = root.isMember("port") ? root["port"].asInt() : DEFAULT_PORT;
        if (root.isMember("db")) {
          db = root["db"].asInt();
        }
        useResp3 = root.isMember("useresp3") ? root["useresp3"].asBool() : true;
        useAuth = root.isMember("useauth") && root.isMember("password") ? root["useauth"].asBool() : false;
        if (useAuth) {
          password = std::optional<std::string>(root["password"].asString());
          if (root.isMember("username")) {
            username = root["username"].asString();
          }
        }
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
          cfg.hostname, cfg.port, cfg.db.value_or(0), cfg.useAuth, cfg.username.value_or("nill"),
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
