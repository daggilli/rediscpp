#ifndef REDISCPP_HPP__
#define REDISCPP_HPP__
#include <hiredis/hiredis.h>
#include <json/reader.h>
#include <json/value.h>

#include <concepts>
#include <csignal>
#include <cstring>
#include <expected>
#include <format>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <print>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "redisconfig.hpp"

namespace RedisCpp {
  inline std::once_flag sigSetup;

  constexpr int tcpTimeoutMillis = 3600 * 1000;
  constexpr auto blockingTimeoutSecs = "1";  // interpreted as double so "0.25" = 250ms
  constexpr auto blockingNoTimeout = "0";

  struct ReplyDeleter {
    void operator()(redisReply *reply) const noexcept {
      if (reply) ::freeReplyObject(reply);
    }
  };

  struct ContextDeleter {
    void operator()(redisContext *ctx) const noexcept {
      std::println("CONTEXTDELETER");
      if (ctx) ::redisFree(ctx);
    }
  };

  using ListenerMap = std::unordered_map<std::string, std::jthread>;
  using Callback = std::function<void(std::string_view, std::string_view)>;
  using ReplyPointer = std::unique_ptr<redisReply, ReplyDeleter>;
  using ContextPointer = std::unique_ptr<redisContext, ContextDeleter>;

  // ensure arguments are string_view or can construct a string_view implicitly. Do not allow
  // rvalue refs (temporaries) because they will dangle
  template <typename T>
  concept satisfies_stringview = std::constructible_from<std::string_view, T> &&
      !(std::same_as<std::remove_cvref_t<T>, std::string> && std::is_rvalue_reference_v<T &&>);

  class Client {
   public:
    explicit Client() = delete;
    explicit Client(const Config &cfg) : config{cfg} {
      Client::setSignals();
      context = createContext(config);
    }
    Client(const Client &cli) = delete;
    Client(Client &&cli) : config{std::move(cli.config)}, context{std::exchange(cli.context, nullptr)} {}
    virtual ~Client() {
      std::println("REDISCLIENT DTOR");
      for (auto &[name, lthread] : listeners) {
        lthread.request_stop();
      }
      listeners.clear();
    }
    Client operator=(const Client &cli) = delete;
    Client &operator=(Client &&cli) {
      config = std::move(cli.config);
      context = std::exchange(cli.context, nullptr);
      return *this;
    }

    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(const char *format, Args &&...args) {
      ReplyPointer replyPtr(
          static_cast<redisReply *>(::redisCommand(context.get(), format, std::forward<Args>(args)...)));
      if (!replyOk(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("command failed: {}", context->err));
      }
      return replyPtr;
    }

    [[nodiscard]] inline ReplyPointer commandArgv(std::span<const std::string_view> args) {
      std::vector<const char *> argv;
      argv.reserve(args.size());
      std::vector<size_t> lens;
      lens.reserve(args.size());
      for (auto sv : args) {
        argv.push_back(sv.data());
        lens.push_back(sv.size());
      }
      ReplyPointer replyPtr(static_cast<redisReply *>(
          ::redisCommandArgv(context.get(), static_cast<int>(argv.size()), argv.data(), lens.data())));
      if (!replyOk(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("commandArgv failed: {}", context->err));
      }
      return replyPtr;
    }

    [[nodiscard]] std::optional<std::string> get(const std::string_view key) {
      auto getReply = commandArgv({"GET", key});
      if (replyOk(context.get(), getReply, REDIS_REPLY_STRING)) {
        return std::optional<std::string>(getReply->str);
      }

      return std::nullopt;
    }

    template <typename... Args>
    [[nodiscard]] inline ReplyPointer set(const char *const key, const char *const value, Args &&...args) {
      auto argv = std::vector<std::string_view>{"SET", key, value};
      gatherArgs(argv, std::forward<Args>(args)...);
      return commandArgv(argv);
    }

    [[nodiscard]] std::vector<std::string> vectoriseReply(ReplyPointer const &reply) {
      std::vector<std::string> replyVec{};
      if (reply->type == REDIS_REPLY_ARRAY) {
        replyVec.reserve(reply->elements);
        for (auto i{0u}; i < reply->elements; i++) {
          replyVec.emplace_back(reply->element[i]->str);
        }
      }
      return replyVec;
    }

    bool addListener(const std::string queueName, Callback cb) {
      auto lock = std::scoped_lock(mx);
      if (auto lthread = listeners.find(queueName); lthread != listeners.end() && lthread->second.joinable())
        return false;

      auto listener = [this, qn = queueName, handler = std::move(cb)](std::stop_token tok) mutable {
        auto ctx = createContext(config);

        while (!tok.stop_requested()) {
          auto replyPtr = commandArgv(ctx.get(), {"BLPOP", qn, blockingTimeoutSecs});
          if (!tok.stop_requested() && bpopReplyOk(context.get(), replyPtr)) {
            auto item = std::make_tuple(std::string_view{replyPtr->element[0]->str},
                                        std::string_view{replyPtr->element[1]->str});
            std::apply(handler, item);
          }
        }
      };

      auto [newThread, wasInserted] = listeners.try_emplace(queueName, listener);
      return wasInserted;
    }

    bool removeListener(const std::string &queueName) {
      auto lock = std::scoped_lock(mx);
      auto lthread = listeners.find(queueName);
      lthread->second.request_stop();
      return listeners.erase(queueName) > 0;
    }

   private:
    Config config;
    ContextPointer context;
    mutable std::mutex mx;
    ListenerMap listeners;

    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(redisContext *ctx, const char *format, Args &&...args) {
      ReplyPointer replyPtr(
          static_cast<redisReply *>(::redisCommand(ctx, format, std::forward<Args>(args)...)));
      if (!replyOk(ctx, replyPtr)) {
        throw std::runtime_error(std::format("command failed: {}", ctx->err));
      }
      return replyPtr;
    }

    [[nodiscard]] inline ReplyPointer commandArgv(redisContext *ctx, std::span<const std::string_view> args) {
      std::vector<const char *> argv;
      argv.reserve(args.size());
      std::vector<size_t> lens;
      lens.reserve(args.size());
      for (auto sv : args) {
        argv.push_back(sv.data());
        lens.push_back(sv.size());
      }
      ReplyPointer replyPtr(static_cast<redisReply *>(
          ::redisCommandArgv(ctx, static_cast<int>(argv.size()), argv.data(), lens.data())));
      if (!replyOk(ctx, replyPtr)) {
        throw std::runtime_error(std::format("commandArgv failed: {}", ctx->err));
      }
      return replyPtr;
    }

    ContextPointer createContext(const Config &cfg) {
      auto ctx = ::redisConnect(cfg.hostname.c_str(), cfg.port);
      if (ctx == nullptr || ctx->err != 0) {
        throw std::runtime_error(std::format("Could not connect to Redis server: {}", ctx->err));
      }
      if (cfg.db.has_value()) {
        auto setDatabase = std::format("SELECT {}", cfg.db.value());
        auto setDatabasePtr = command(ctx, setDatabase.c_str());

        if (setDatabasePtr == nullptr || setDatabasePtr->type != REDIS_REPLY_STATUS ||
            setDatabasePtr->len != 2 || std::strncmp(setDatabasePtr->str, "OK", 2)) {
          throw std::runtime_error("Could not set database");
        }
      }
      if ((::redisEnableKeepAlive(ctx)) < 0) {
        throw std::runtime_error("Enable keep-alive failed");
      }
      if ((::redisSetTcpUserTimeout(ctx, tcpTimeoutMillis)) < 0) {
        throw std::runtime_error("Set TCP timeout failed");
      }

      return ContextPointer(ctx);
    }

    bool replyOk(redisContext *ctx, ReplyPointer const &reply) noexcept {
      if (reply == nullptr || ctx->err) return false;
      return true;
    }
    bool replyOk(redisContext *ctx, ReplyPointer const &reply, int expectedType) noexcept {
      if (reply == nullptr || ctx->err || reply->type != expectedType) return false;
      return true;
    }

    bool bpopReplyOk(redisContext *ctx, ReplyPointer const &reply) {
      if (reply == NULL || ctx->err) return false;
      if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 2) return false;
      return true;
    }

    template <satisfies_stringview... Args>
    void gatherArgs(std::vector<std::string_view> &onto, Args &&...args) {
      onto.reserve(onto.size() + sizeof...(Args));
      (onto.emplace_back(args), ...);
    }

    static inline void setSignals() {
      std::call_once(sigSetup, [] {
        struct sigaction sigAct {};
        sigAct.sa_flags = 0;
        sigemptyset(&sigAct.sa_mask);
        sigAct.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &sigAct, nullptr);
      });
    }
  };
};  // namespace RedisCpp
#endif
