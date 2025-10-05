#ifndef REDISCPP_HPP__
#define REDISCPP_HPP__
#include <hiredis/hiredis.h>
#include <json/reader.h>
#include <json/value.h>
#include <poll.h>
#include <sys/eventfd.h>

#include <algorithm>
#include <concepts>
#include <csignal>
#include <cstring>
#include <expected>
#include <format>
#include <fstream>
#include <functional>
#include <future>
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

//
using namespace std::string_literals;
//
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

  struct Subscriber {
    std::jthread subThread;
    std::shared_future<void> finished;
  };

  using ListenerMap = std::unordered_map<std::string, std::jthread>;
  using Callback = std::function<void(std::string_view, std::string_view)>;
  using ReplyPointer = std::unique_ptr<redisReply, ReplyDeleter>;
  using ContextPointer = std::unique_ptr<redisContext, ContextDeleter>;
  using SubscriberCallback = std::function<void(std::span<std::string_view>)>;
  using SubscriberVec = std::vector<Subscriber>;

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

      auto subLock = std::scoped_lock(subscriberMutex);
      for (auto &&sub : subscriberVec) {
        sub.subThread.request_stop();
      }
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
      auto lock = std::scoped_lock(listenerMutex);
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
      auto lock = std::scoped_lock(listenerMutex);
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

      auto allChannels = [&]<std::size_t... I>(std::index_sequence<I...>)->std::vector<std::string_view> {
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

      auto [channels, patChannels] =
          [this](
              auto &channelVec) -> std::pair<std::vector<std::string_view>, std::vector<std::string_view>> {
        std::vector<std::string_view> chans, patChans;
        chans.reserve(channelVec.size() + 1);
        patChans.reserve(channelVec.size() + 1);

        for (auto &c : channelVec) {
          auto &v = isSubscribePattern(c) ? patChans : chans;
          v.emplace_back(c);
        }
        if (!chans.empty()) chans.insert(chans.begin(), "SUBSCRIBE");
        if (!patChans.empty()) patChans.insert(patChans.begin(), "PSUBSCRIBE");
        return std::make_pair(chans, patChans);
      }(allChannels);

      auto subPromise = std::make_shared<std::promise<void>>();
      auto finished = subPromise->get_future().share();

      auto subscriber = [this, chans = std::move(channels), patChans = std::move(patChannels),
                         handler = std::move(cb), subPromise](std::stop_token tok) mutable {
        auto ctx = connectAndSubscribe(config, chans, patChans);

        auto contextFileDesc = ctx->fd;
        int eventFileDesc;
        if ((eventFileDesc = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
          try {
            subPromise->set_value();
          } catch (...) {
          }
          return;
        }

        pollfd pfds[2] = {{contextFileDesc, POLLIN, 0}, {eventFileDesc, POLLIN, 0}};

        {
          std::stop_callback onRequestStop(tok, [efd = eventFileDesc] {
            std::println("STOP CB");
            uint64_t one = 1;
            (void)::write(efd, &one, sizeof(one));
          });

          if (tok.stop_requested()) {
            uint64_t one = 1;
            (void)::write(eventFileDesc, &one, sizeof(one));
          }

          while (true) {
            int pollResult;
            do {
              pollResult = ::poll(pfds, 2, -1);  // block; eventfd wakes us
            } while (pollResult < 0 && errno == EINTR);

            if (pollResult == 0) continue;  // timeout

            if (pollResult < 0) {
              if (errno == EINTR) continue;
              break;
            }

            if (pfds[1].revents & POLLIN) {
              std::println("STOP ON WAKE");
              uint64_t tmp;
              (void)::read(eventFileDesc, &tmp, sizeof tmp);
              std::println("WAKE EXIT");
              break;
            }

            // socket died: attempt to reccreate context with exponential backoff
            if (pfds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
              std::chrono::milliseconds backoff{100};
              auto maxRetries{8u};
              auto retries{0u};

              do {
                ctx = connectAndSubscribe(config, chans, patChans);
                if (ctx->err) {
                  std::this_thread::sleep_for(backoff);
                  retries++;
                  backoff *= 2;
                  continue;
                }
              } while (ctx->err && retries < maxRetries);
              if (ctx->err || retries == maxRetries) break;

              pfds[0].fd = ctx->fd;
              continue;
            }

            if (pfds[0].revents & POLLIN) {  // readable
              handleSubscriptionMessage(ctx.get(), std::move(handler));
            }
          }
        }
        std::println("EXIT THREAD");
        (void)::close(eventFileDesc);
        try {
          subPromise->set_value();
        } catch (...) {
        }
      };

      {
        auto subLock = std::scoped_lock(subscriberMutex);
        subscriberVec.emplace_back(Subscriber{std::jthread(std::move(subscriber)), finished});
      }
      return true;
    }

   private:
    Config config;
    ContextPointer context;
    mutable std::mutex listenerMutex;
    ListenerMap listeners;
    mutable std::mutex subscriberMutex;
    SubscriberVec subscriberVec;

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

    inline int appendCommandArgv(redisContext *ctx, std::span<const std::string_view> args) {
      if (args.empty()) return 0;
      std::vector<const char *> argv;
      argv.reserve(args.size());
      std::vector<size_t> lens;
      lens.reserve(args.size());
      for (auto sv : args) {
        argv.push_back(sv.data());
        lens.push_back(sv.size());
      }
      return ::redisAppendCommandArgv(ctx, static_cast<int>(argv.size()), argv.data(), lens.data());
    }

    ContextPointer connectAndSubscribe(const Config &config, const std::vector<std::string_view> &channels,
                                       const std::vector<std::string_view> &patChannels) {
      auto ctx = createContext(config, false);
      (void)appendCommandArgv(ctx.get(), channels);
      (void)flushPending(ctx.get());
      (void)appendCommandArgv(ctx.get(), patChannels);
      (void)flushPending(ctx.get());
      return ctx;
    }

    inline bool flushPending(redisContext *ctx) {
      int done = 0;
      while (!done) {
        auto rc = ::redisBufferWrite(ctx, &done);
        if (rc == REDIS_ERR) return false;
      }
      return true;
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
      if (cfg.useAuth && cfg.password.has_value()) {
        auto auth = std::format("AUTH {} {}", cfg.username.value_or("default"), cfg.password.value());
        auto authPtr = command(ctx, auth.c_str());
        if (!statusReplyOk(ctx, authPtr)) {
          throw std::runtime_error("Could not authenticate");
        }
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

    void handleSubscriptionMessage(redisContext *ctx, SubscriberCallback &&handler) {
      void *reply = nullptr;
      if (::redisGetReply(ctx, &reply) == REDIS_OK) {
        auto replyPtr = ReplyPointer(static_cast<redisReply *>(reply));
        auto elements = extractMessageElements(replyPtr);
        handler(elements);
      }

      // drain any further messages
      for (;;) {
        void *reply = nullptr;
        int rc = redisGetReplyFromReader(ctx, &reply);
        if (rc == REDIS_ERR || reply == nullptr) break;

        auto replyPtr = ReplyPointer(static_cast<redisReply *>(reply));
        auto elements = extractMessageElements(replyPtr);
        handler(elements);
      }
    }

    std::vector<std::string_view> extractMessageElements(ReplyPointer const &replyPtr) {
      std::vector<std::string_view> elementVec;
      elementVec.reserve(replyPtr->elements);
      auto elems = replyPtr->element;

      for (auto i{0u}; i < replyPtr->elements; i++) {
        if (elems[i]->type == REDIS_REPLY_STRING) {
          elementVec.emplace_back(elems[i]->str);
        }
      }

      return elementVec;
    }

    void reapSubscribers() {
      auto subLock = std::scoped_lock(subscriberMutex);
      std::erase_if(subscriberVec, [](Subscriber &sub) {
        if (sub.finished.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
          if (sub.subThread.joinable()) sub.subThread.join();
          std::println("REAP TRUE");
          return true;  // erase
        }
        std::println("REAP FALSE");
        return false;  // keep
      });
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

    bool isSubscribePattern(const std::string_view &str) {
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
