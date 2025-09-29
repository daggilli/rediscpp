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

  class Config {
   public:
    explicit Config(const std::string &hostname_, int port_, int db_ = 0)
        : hostname{std::move(hostname_)}, port{port_}, db{db_} {};
    Config(const Config &config) = default;
    Config(Config &&config) = default;
    Config(const fs::path &configFilePath) {
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

        hostname = root["hostname"].asString();
        port = root["port"].asInt();
        db = root.isMember("db") ? std::optional<int>(root["db"].asInt()) : std::nullopt;
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
  };
};  // namespace RedisCpp
#endif