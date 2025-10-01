#ifndef REDISCPP_HPP__
#define REDISCPP_HPP__
#include <hiredis/hiredis.h>
#include <json/reader.h>
#include <json/value.h>

#include <algorithm>
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
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "redisconfig.hpp"

using namespace std::chrono_literals;

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
  using SubscriberCallback = std::function<void(std::string_view, std::string_view, std::string_view)>;

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

    [[nodiscard]] inline ReplyPointer publish(const char *const channel, const char *const message) {
      return commandArgv({"PUBLISH", channel, message});
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

    template <typename... Args>
    requires(sizeof...(Args) >= 2) bool subscribe(Args &&...args) {
      auto argsTuple = std::forward_as_tuple(std::forward<Args>(args)...);
      constexpr std::size_t N = sizeof...(Args);

      using CallbackArg = std::tuple_element_t<N - 1, decltype(argsTuple)>;
      static_assert(std::constructible_from<SubscriberCallback, std::decay_t<CallbackArg>>,
                    "subscribe: last argument must be a callable convertible to SubscriberCallback");
      SubscriberCallback cb{std::get<N - 1>(argsTuple)};

      auto channels = [&]<std::size_t... I>(std::index_sequence<I...>)->std::vector<std::string_view> {
        using argDecltype = decltype(argsTuple);
        static_assert(((std::convertible_to<std::decay_t<std::tuple_element_t<I, argDecltype>>,
                                            std::string_view>)&&...),
                      "subscribe: channel args must be string_view-like");
        static_assert(
            (!(std::is_same_v<std::remove_cvref_t<std::tuple_element_t<I, argDecltype>>, std::string> &&
               std::is_rvalue_reference_v<std::tuple_element_t<I, argDecltype>>)&&...),
            "subscribe: rvalue std::string not allowed (temporary, would dangle)");
        return std::vector<std::string_view>{std::string_view{std::get<I>(argsTuple)}...};
      }
      (std::make_index_sequence<N - 1>{});

      channels.insert(channels.begin(), "SUBSCRIBE");

      for (auto &c : channels) {
        std::println(" CH: {}", c);
      }

      auto subscriber = [this, chans = std::move(channels),
                         handler = std::move(cb)](std::stop_token tok) mutable {
        auto ctx = createContext(config, false);
        timeval tv{0, 200'000};  // 200 ms
        ::redisSetTimeout(ctx.get(), tv);

        auto subReplyPtr = commandArgv(ctx.get(), chans);

        auto ix{0u};
        while (!tok.stop_requested()) {
          errno = 0;
          redisReply *reply = nullptr;
          auto result = ::redisGetReply(ctx.get(), std::bit_cast<void **>(&reply));
          // std::println("==== RESULT {} {}", result, rx++);

          if (result == REDIS_OK) {
            auto replyPtr = ReplyPointer(static_cast<redisReply *>(reply));  // RAII
            // handle pub/sub frame (RESP2 arrays: ["message", ch, payload], etc.)
            std::println("SUB RCV OK {}", ix++);
            auto elems = replyPtr->element;
            if (replyPtr->element[2]->type == REDIS_REPLY_STRING) {
              handler(elems[0]->str, elems[1]->str, elems[2]->str);
            }
            continue;
          }

          const int e = errno;
          if (e == EAGAIN || e == EWOULDBLOCK || e == EINTR) {
            // Just a timeout (or interrupted read) — not fatal. Go around and try again.
            ctx->err = 0;
            ctx->errstr[0] = '\0';
            continue;
          }

          break;
          // auto replyPtr = getReply(ctx.get());
          // if (replyPtr) {
          //   if (!subscribeReplyOk(ctx.get(), replyPtr)) {
          //     throw std::runtime_error("Subscription read failed");
          //   }
          //   auto elems = replyPtr->element;

          //   if (replyPtr->element[2]->type == REDIS_REPLY_STRING) {
          //     handler(elems[0]->str, elems[1]->str, elems[2]->str);
          //   }
          // }
        }
      };

      subscriberVec.emplace_back(subscriber);
      return true;
    }

   private:
    Config config;
    ContextPointer context;
    mutable std::mutex mx;
    ListenerMap listeners;
    std::vector<std::jthread> subscriberVec;

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

    [[nodiscard]] inline ReplyPointer getReply(redisContext *ctx) {
      redisReply *reply;
      auto result = ::redisGetReply(ctx, std::bit_cast<void **>(&reply));
      std::println("RESULT {}", result);
      if (result != REDIS_OK) {
        if (ctx->err && ctx->err != REDIS_ERR_IO) {
          throw std::runtime_error(
              std::format("getReply failed (return value != {} {} {}", REDIS_OK, result, ctx->err));
        }
        return ReplyPointer(nullptr);
      }
      return ReplyPointer(reply);
    }

    ContextPointer createContext(const Config &cfg, bool allowResp3 = true) {
      auto ctx = ::redisConnect(cfg.hostname.c_str(), cfg.port);
      if (ctx == nullptr || ctx->err != 0) {
        throw std::runtime_error(std::format("Could not connect to Redis server: {}", ctx->err));
      }
      if (cfg.db.has_value()) {
        auto setDatabase = std::format("SELECT {}", std::clamp(cfg.db.value(), 0, 16));
        auto setDatabasePtr = command(ctx, setDatabase.c_str());
        if (!statusReplyOk(ctx, setDatabasePtr)) {
          throw std::runtime_error("Could not set database");
        }
      }
      if (allowResp3 && cfg.useResp3.value_or(false)) {
        auto setResp3Ptr = command(ctx, "HELLO 3");
        if (!replyOk(ctx, setResp3Ptr)) {
          throw std::runtime_error("Could not upgrade to RESP3");
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

    inline bool replyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err);
    }

    inline bool replyOk(redisContext *ctx, ReplyPointer const &replyPtr, int expectedType) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == expectedType);
    }

    inline bool statusReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_STATUS &&
              replyPtr->len == 2 && !std::strncmp(replyPtr->str, "OK", 2));
    }

    inline bool statusReplyMapOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_MAP &&
              replyPtr->len % 2 == 0 && !std::strncmp(replyPtr->str, "OK", 2));
    }

    inline bool subscribeReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_ARRAY &&
              replyPtr->elements == 3);
    }

    inline bool bpopReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_ARRAY &&
              replyPtr->elements == 2);
    }

    template <satisfies_stringview... Args>
    void gatherArgs(std::vector<std::string_view> &onto, Args &&...args) {
      onto.reserve(onto.size() + sizeof...(Args));
      (onto.emplace_back(args), ...);
    }

    bool isSubscribePattern(const std::string_view str) {
      std::ostringstream escaped;
      for (auto i = str.begin(); i < str.end(); i++) {
        if (*i == '\\') {  // ignore literal slash and subsequent character
          i++;
          continue;
        }
        escaped << *i;
      }

      return escaped.str().find_first_of("[]?*") != std::string::npos;
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
