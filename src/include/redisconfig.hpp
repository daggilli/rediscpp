#ifndef REDISCONFIG_HPP__
#define REDISCONFIG_HPP__
#include <json/reader.h>
#include <json/value.h>

#include <filesystem>
#include <format>
#include <fstream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace RedisCpp {
  namespace fs = std::filesystem;

  constexpr const char *const DEFAULT_HOST = "localhost";
  constexpr int DEFAULT_PORT = 6379;

  class Config {
   public:
    explicit Config(const std::string &hostname_, int port_, int db_ = 0,
                    std::optional<std::string> un_ = std::nullopt,
                    std::optional<std::string> pw_ = std::nullopt)
        : hostname{std::move(hostname_)}, port{port_}, db{db_}, username(un_), password(pw_){};
    Config(const Config &config) = default;
    Config(Config &&config) = default;
    Config(const fs::path &configFilePath)
        : db(std::nullopt), username(std::nullopt), password(std::nullopt) {
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
        useResp3 = root.isMember("useresp3") ? std::optional<bool>(root["useresp3"].asBool()) : std::nullopt;
        useAuth = root.isMember("useauth") && root.isMember("password") ? root["useauth"].asBool() : false;
        if (useAuth) {
          password = std::optional<std::string>(root["password"].asString());
          if (root.isMember("username")) {
            username = root["username"].asString();
          }
        }
      }
    }
    Config &operator=(const Config &config) {
      if (this != &config) {
        hostname = config.hostname;
        port = config.port;
        db = config.port;
      }
      return *this;
    }
    Config &operator=(Config &&config) {
      if (this != &config) {
        hostname = std::move(config.hostname);
        port = config.port;
        db = config.db;
      }
      return *this;
    }
    std::string hostname;
    int port;
    std::optional<int> db;
    std::optional<bool> useResp3;
    bool useAuth;
    std::optional<std::string> username;
    std::optional<std::string> password;
  };
};  // namespace RedisCpp
#endif