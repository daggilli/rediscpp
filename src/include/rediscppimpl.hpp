#pragma once

#include <hiredis/hiredis.h>
#include <json/reader.h>
#include <json/value.h>
#include <poll.h>
#include <sys/eventfd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <concepts>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <format>
#include <fstream>
#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <mutex>
#include <optional>
#include <print>
#include <random>
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

// C++26 introduced a constructor for std::span from an initializer_list
// If we don't have it, enable some shim overloads
#if defined(__cpp_lib_span_initializer_list) && __cpp_lib_span_initializer_list >= 202311L
#define HAS_SPAN_INIT_LIST_CTOR 1
#else
#define HAS_SPAN_INIT_LIST_CTOR 0
#include <initializer_list>
#endif

#include "rediscppconfig.hpp"

namespace RedisCpp {
  namespace fs = std::filesystem;
  using namespace std::string_literals;
  using namespace std::chrono_literals;

  inline std::once_flag sigSetup;

  constexpr unsigned tcpTimeoutMillis = 3600 * 1000;
  constexpr unsigned defaultBlockingTimeoutMillis = 1000;
  constexpr unsigned minBlockingTimeoutMillis = 50;
  constexpr unsigned maxBlockingTimeoutMillis = tcpTimeoutMillis / 2;
  constexpr auto blockingNoTimeout = "0";

  /**
   * @brief The custom deleter for the RAII `std::unique_ptr` that encapsulates a `redisReply` pointer
   *
   */
  struct ReplyDeleter {
    void operator()(redisReply *reply) const noexcept {
      if (reply) ::freeReplyObject(reply);
    }
  };

  /**
   * @brief The custom deleter for the RAII `std::unique_ptr` that encapsulates a `redisContext` pointer
   *
   */
  struct ContextDeleter {
    void operator()(redisContext *ctx) const noexcept {
      if (ctx) ::redisFree(ctx);
    }
  };

  /**
   * @brief Command words for communication with running subscriber instances
   *
   */
  enum class Command { SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, UNSUBSCRIBEALL, TERMINATE };

  /**
   * @brief A message object that can be passed to subscriber instances
   *
   * @struct Message
   */
  struct Message {
    Command command;                        /**< The command words for the message */
    std::vector<std::string_view> channels; /**< a list of channels (possibly zero-length) */
  };

  /**
   * @brief A message queue for communication with running subscribers
   *
   * @struct MessageQueue
   *
   *
   * @details A `MessageQueue` object is the means by which an application can communicate with its running
   * subscribers. An instance of a `MessageQueue` is stored in a `std::shared_ptr` inside an instance of
   * the `Subscriber` class. Client member functions visible to the application such as `subscribe()`
   * place objects of type `Message` into the queue and signal the subscriber to process it by writing
   * to the queue's file descriptor with `MessageQueue::wakeup()`, which the subscriber is moonitoring via
   * `poll()`. Thread safety is provided by a mutex on which a function attempting to read or write from the
   * queue must obtain a lock.
   *
   */
  struct MessageQueue {
    /**
     * @brief Drain the queue's `eventfd` file descriptor to reset it
     *
     */
    void drain() {
      uint64_t tmp;
      (void)::read(signalFileDesc, &tmp, sizeof(tmp));
    }
    /**
     * @brief Write to the queue's `eventfd` file descriptor to alert the subcriber
     * that the queue should be read
     *
     */
    void wakeup() {
      uint64_t one{1};
      (void)::write(signalFileDesc, &one, sizeof(one));
    }
    int signalFileDesc{-1};            /**< The `eventfd` file descriptor to signal queue data is ready */
    std::mutex queueMutex;             /**< A lock on queuen access */
    std::deque<Message> messages;      /**< The queue */
    std::atomic_bool terminate{false}; /**< A flag to signal the subscriber to shut dowm */
  };

  /**
   * @brief An object to handle subscriptions to pob/sub channels
   *
   * @details The `Subscriber` object manages subscriptions to Redis publish and subscribe channels. It
   * listens for incoming messages and despatches them to a callable object for further processing.
   * Once started, a `Subscriber` runs in an asynchronous context (one thread per subscriber). Communications
   * between the main thread in which the subscriber was started and the running thread is via a shared
   * message queue of type `MessageQueue`. Before reading to or writing from the queue, it is locked with a
   * mutex held in the `MessageQueue` object.
   *
   */
  struct Subscriber {
    std::jthread subThread;                 /**< The theead in which the subscriber's main body runs */
    std::shared_future<void> finished;      /**< A flag to indicate the subscriber has ceased operations */
    std::shared_ptr<MessageQueue> queuePtr; /**< A pointer to the message queue object */
    std::latch ready{1}; /**< A one-shot latch to indicate the subscriber has completed initialisation */
  };

  struct RandomGenerator {
    uint64_t operator()() { return std::uniform_int_distribution<uint64_t>{}(engine); }

    std::mt19937 engine{std::random_device{}()};
  };

  using ListenerMap = std::unordered_map<std::string, std::jthread>;
  using ListenerCallback = std::function<void(std::string_view, std::string_view)>;
  using ReplyPointer = std::unique_ptr<redisReply, ReplyDeleter>;
  using ContextPointer = std::unique_ptr<redisContext, ContextDeleter>;
  using SubscriberCallback = std::function<void(std::span<std::string_view>)>;
  using SubscriberMap = std::unordered_map<uint64_t, Subscriber>;
  using HmapEntry = std::pair<std::string, std::string>;
  using HmapVec = std::vector<HmapEntry>;
  using HmapItem = std::pair<std::string, std::optional<std::string>>;
  using HmapItemVec = std::vector<HmapItem>;
  using ScriptMap = std::unordered_map<std::string, std::string>;

  /**
   * Ensure all elements of a parameter pack are `std::string_view` or capable of being
   * converted to a `std::string_view`. Disallow rvalue `std::string` entries (*i.e.* temporaries)
   * as these will go out of scope and dangle.
   */
  template <typename T>
  concept satisfies_stringview =
      std::constructible_from<std::string_view, T> &&
      !(std::same_as<std::remove_cvref_t<T>, std::string> && std::is_rvalue_reference_v<T &&>);

  /**
   * @class Client
   *
   * @brief A Redis client class that provides most commonly-used features
   *
   * @details The `Client` class encapsulates a Redis client connection with functionality to get and set
   * values, manipulate lists and sets, perform blocking listens on lists, publish and subscribe, and
   * loading/evaluation of Lua scripts.
   *
   */
  class Client {
   public:
    explicit Client() = delete; /**< Default constructor deleted */

    /**
     * @brief Construct a new Client object with a supplied configuration
     *
     * @param cfg A configuration object of type `Config`
     */
    explicit Client(const Config &cfg) : config{cfg} {
      Client::setSignals();
      context = createContext(config);
      startReaper();
    }

    virtual ~Client() {
      for (auto &[name, lthread] : listeners) {
        lthread.request_stop();
      }
      listeners.clear();
    }

    /**
     * Copy and move contructors and assignments are deleted because the class contains mutexes
     *
     */
    Client(const Client &) = delete;           /**< Class cannot be copied */
    Client(Client &&) = delete;                /**< Class cannot be moved */
    Client operator=(const Client &) = delete; /**< Copy assignment deleted */
    Client &operator=(Client &&) = delete;     /**< Move assignment deleted */

    /**
     * @brief Send a command to Redis
     *
     * @param format A format string to which `args` will be applied
     * @param args A paramter pack containing arguments for the command
     *
     * @return The reply from Redis
     *
     * @details This function exposes the `redisCommand` call that can be used to send an arbitrary command to
     * Redis. The `format` parameter is a string-like object with a command, options and format specifiers
     * that will be populated with the parameters given in `args` before being sent to the Redis engine. Redis
     * expects a C-style zero-terminated string for `format`, so overloads for this function have been
     * provided so that it can be called with string-like objects (`std::string`, `std::string_view`) without
     * having to worry about the compiler trying to implicitly convert the format string and failing overload
     * resolution.
     *
     */
    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(const char *format, Args &&...args) {
      ReplyPointer replyPtr(
          static_cast<redisReply *>(::redisCommand(context.get(), format, std::forward<Args>(args)...)));
      if (!replyOk(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("command failed: {}", context->err));
      }
      return replyPtr;
    }

    /**
     * @brief Overload for `std::string` format
     *
     */
    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(const std::string &format, Args &&...args) {
      return command(format.c_str(), std::forward<Args>(args)...);
    }

    /**
     * @brief Overload for rvalue `std::string` format
     *
     */
    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(const std::string &&format, Args &&...args) {
      auto fstr = std::move(format);
      return command(fstr.c_str(), std::forward<Args>(args)...);
    }

    /**
     * @brief Overload for `std::string_view` format
     *
     * @details This overload is necessary because `std::string_view` does not necessarily point to a
     * null-terminated C-style string, but the underlying `redisCommand()` function takes such a string (in
     * fact a pointer to `char`). `std::string` has a constructor that can take a `std::string_view` and
     * produce a string whose underlying null-terminated representation can be accessed by
     * `std::string::c_str`.
     *
     */
    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(const std::string_view format, Args &&...args) {
      auto fstr = std::string(format);
      return command(fstr.c_str(), std::forward<Args>(args)...);
    }

    /**
     * @brief Send a command to Redis (ARGV fornat)
     *
     * @param args A span (or a container convertible to span) containing the cammand and parameters
     *
     * @return The reply from Redis
     *
     * @details This function exposes the `redisCommandArgv` call that can be used to send an arbitrary
     * command to Redis. It takes a span-like set of `std::string_view` arguments. This can be an explicit
     * `std::span` or a container such as `std::array` or `std::vector` that can be implicitly converted to a
     * span. An initialiser list can be used to construct a span in C++26 but for C++23 an overload is
     * provided to enable this behaviour.
     *
     */
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

#if !HAS_SPAN_INIT_LIST_CTOR
    [[nodiscard]] inline ReplyPointer commandArgv(std::initializer_list<std::string_view> args) {
      return commandArgv(std::span<const std::string_view>(args.begin(), args.size()));
    }
#endif

    /**
     * @brief Get a value from Redis by its key
     *
     * @param key The Redis key
     *
     * @return a `std::optional` containing the value as a string or `std::nullopt`
     *
     */
    [[nodiscard]] std::optional<std::string> get(const std::string_view key) & {
      auto getReply = commandArgv({"GET", key});
      if (stringReplyOk(context.get(), getReply)) {
        return std::string(getReply->str, getReply->len);
      }

      return std::nullopt;
    }

    /**
     * @brief Set the value of a key, with options
     *
     * @param key The Redis key
     * @param value The value to set for the key
     * @param args Additional parameters *e.g.* expiry time
     *
     * @return The reply from Redis (see documentation for possible values)
     *
     * @details This function sets a string value. It optionally takes additional parameters such as `EX` to
     * set expiry time-to-live or `GET` to return the previously stored value. The exact value of the reply
     * depends on whether the `GET` parameter was supplied (see the official Redis documentation).
     *
     */
    template <satisfies_stringview... Args>
    [[nodiscard]] inline ReplyPointer set(const std::string_view key, const std::string_view value,
                                          Args &&...args) {
      constexpr size_t N = 3 + sizeof...(Args);
      auto argv =
          std::array<std::string_view, N>{"SET", key, value, std::string_view(std::forward<Args>(args))...};
      return commandArgv(argv);
    }

    /**
     * @brief Determine whether a key or keys with the given name exist(s)
     *
     * @param args A set of key names whose existence is to be determined
     *
     * @return int The number of keys with the given names that exist
     *
     * @details The return value of this function is an integer that counts how many of the supplied keys
     * exist in the database. It does not provide any information on which of the keys exist if more than one
     * key is supplied. It can only report the information 'some' or 'none' (this is a limitation of the Redis
     * engine).
     *
     */
    template <satisfies_stringview... Args>
    [[nodiscard]] inline int exists(Args &&...args) & {
      constexpr size_t N = 1 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{"EXISTS", std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (!intReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("EXISTS failed");
      }
      return replyPtr->integer;
    }

    [[nodiscard]] inline std::vector<std::string> keys(const std::string_view pattern) & {
      auto argv = std::array<std::string_view, 2>{"KEYS", pattern};
      auto replyPtr = commandArgv(argv);
      if (!arrayLikeReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("KEYS failed");
      }
      std::vector<std::string> keyVec;
      if (replyPtr->elements == 0) return keyVec;

      keyVec.reserve(replyPtr->elements);

      for (auto i{0u}; i < replyPtr->elements; i++) {
        keyVec.emplace_back(replyPtr->element[i]->str, replyPtr->element[i]->len);
      }

      return keyVec;
    }

    /**
     * @brief Delete items given their keys
     *
     * @param A list of keys to be deleted
     *
     * @return An integer indicating the number of keys that were deleted
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) >= 1)
    [[nodiscard]] inline int del(Args &&...args) & {
      constexpr size_t N = 1 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{"DEL", std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Set key/value pairs in a hash map
     *
     * @param key The key of the hashmap
     * @param args A list of key/value pairs in the form `<key 1> <value 1> <key 2> <value 2> ... <key n>
     * <value n>`
     *
     * @return The number of fields that were added
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0)
    [[nodiscard]] inline int hset(const std::string_view key, Args &&...args) & {
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{"HSET", key, std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Get a value from a hash map field by key
     *
     * @param key The key of the has
     * @param field The name of the firls
     *
     * @return The value contained in the field or `std::nullopt` if no such field exists
     *
     */
    [[nodiscard]] inline std::optional<std::string> hget(const std::string_view key,
                                                         const std::string_view field) & {
      auto argv = std::array<std::string_view, 3>{"HGET", key, field};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_STRING) return std::nullopt;
      return std::string(replyPtr->str, replyPtr->len);
    }

    /**
     * @brief Get multiple items from a hash map
     *
     * @param key The key of the hash map
     * @param args A list of fields in the hash map whose values are required
     *
     * @return A vector of pairs of string and optional string
     *
     * The return value is a `std::vector` of `std::pair` items, each of which is a `std::string` and a
     * `std::optional<std::string>`. The first element of the pair is the field name passed into the function,
     * and the second element is the value for the hash map corresponding to that field, or `std::nullopt` if
     * no such field exists.
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) > 0)
    [[nodiscard]] inline HmapItemVec hmget(const std::string_view key, Args &&...args) {
      auto argsVec = std::vector<std::string_view>{std::forward<Args>(args)...};
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv =
          std::array<std::string_view, N>{"HMGET", key, std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (!arrayLikeReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("HMGET failed");
      }

      HmapItemVec itemVec;
      itemVec.reserve(replyPtr->elements);
      for (auto i{0u}; i < replyPtr->elements; i++) {
        auto *elem = replyPtr->element[i];

        if (elem && elem->type == REDIS_REPLY_STRING) {
          itemVec.emplace_back(
              std::piecewise_construct, std::forward_as_tuple(argsVec[i]),
              std::forward_as_tuple(std::in_place, elem->str, static_cast<size_t>(elem->len)));
        } else {
          itemVec.emplace_back(std::piecewise_construct, std::forward_as_tuple(argsVec[i]),
                               std::forward_as_tuple(std::nullopt));
        }
      }
      return itemVec;
    }

    /**
     * @brief Return all key/value pairs in a hash map
     *
     * @param key The key of the hash map
     *
     * @return A `std::vector` of pairs of `std::string`, or `std::nullopt` if the hash map does not exist
     *
     */
    [[nodiscard]] inline std::optional<HmapVec> hgetall(const std::string_view key) {
      auto argv = std::array<std::string_view, 2>{"HGETALL", key};
      auto replyPtr = commandArgv(argv);
      if (!arrayLikeReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("commandArgv failed: {}", context->err));
      }
      if (replyPtr->elements == 0) return std::nullopt;

      HmapVec hashVector;
      hashVector.reserve(replyPtr->elements / 2);
      for (auto i{0u}; i < replyPtr->elements; i += 2) {
        auto *k = replyPtr->element[i];
        auto *v = replyPtr->element[i + 1];

#ifdef DEBUG
        assert(k != nullptr);
        assert(v != nullptr);
        assert(k->type == REDIS_REPLY_STRING);
        assert(v->type == REDIS_REPLY_STRING);
#endif

        hashVector.emplace_back(std::piecewise_construct,
                                std::forward_as_tuple(k->str, static_cast<size_t>(k->len)),
                                std::forward_as_tuple(v->str, static_cast<size_t>(v->len)));
      }
      return hashVector;
    }

    /**
     * @brief Remove fields from a hash map
     *
     * @param key The key of the hash map
     * @param args A list of fields that re to be removed
     *
     * @return The number of fields that were deleted
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) >= 1)
    [[nodiscard]] inline int hdel(const std::string_view key, Args &&...args) & {
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{"HDEL", key, std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Add a member to a set
     *
     * @param key The key of the set
     * @param args A list of members to be added
     *
     * @return The number of items that were added to the set, not including the elements already present.
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) >= 1)
    [[nodiscard]] inline int sadd(const std::string_view key, Args &&...args) & {
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{"SADD", key, std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Remove elements from a set
     *
     * @param key The key of the set
     * @param args The elements that are to be removed from the set
     *
     * @return The number of elements that were removed from the set
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) >= 2)
    [[nodiscard]] inline int srem(const std::string_view key, Args &&...args) & {
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{"SREM", key, std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Get all the members of a set
     *
     * @param key The key of the set
     *
     * @return A `std::vector` of `std::string`, or `std::nullopt` if no elements were returned
     *
     */
    [[nodiscard]] inline std::optional<std::vector<std::string>> smembers(const std::string_view key) & {
      auto argv = std::array<std::string_view, 2>{"SMEMBERS", key};
      auto replyPtr = commandArgv(argv);
      bool (Client::*okFunc)(redisContext *ctx, ReplyPointer const &replyPtr) =
          config.useResp3 ? &Client::setReplyOk : &Client::arrayLikeReplyOk;
      if (!(this->*okFunc)(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("SMEMBERS failed: {}", context->err));
      }
      if (replyPtr->elements == 0) return std::nullopt;

      std::vector<std::string> setVector;
      setVector.reserve(replyPtr->elements);
      for (auto i{0u}; i < replyPtr->elements; i++) {
        auto *m = replyPtr->element[i];
        setVector.emplace_back(m->str, m->len);
      }
      return setVector;
    }

    /**
     * @brief Determine if a value is a member of a set
     *
     * @param key The key of the set
     * @param member The element to be queried
     *
     * @return A boolean value indicating whether the member was present in the set
     *
     */
    [[nodiscard]] inline bool sismember(const std::string_view key, const std::string_view member) & {
      auto argv = std::array<std::string_view, 3>{"SISMEMBER", key, member};
      auto replyPtr = commandArgv(argv);
      if (!intReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("SISMEMBER failed: {}", context->err));
      }
      return static_cast<bool>(replyPtr->integer);
    }

    /**
     * @brief Count the number of members of a set
     *
     * @param key
     *
     * @return int The nuimber of members of a set or zero if the key was not found. Throws if the key is not
     * a SET type
     */
    [[nodiscard]] inline int scard(const std::string_view key) & {
      auto argv = std::array<std::string_view, 2>{"SCARD", key};
      auto replyPtr = commandArgv(argv);
      if (!intReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error(std::format("SCARD failed: {}", context->err));
      }
      return replyPtr->integer;
    }

    /**
     * @brief Push values onto the right-hand side of a list
     *
     * @param key The key of the list
     * @param args A list of items to be pushed onto the list
     *
     * @return The length of the list after the push operation
     *
     * @detail The queue listeners pop items from the left-hand side of a list, so this is the function to
     * use for standard FIFO queue behaviour.
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) > 0)
    [[nodiscard]] inline int rpush(const std::string_view key, Args &&...args) & {
      auto replyPtr = push("RPUSH", key, std::forward<Args>(args)...);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Push values onto the left-hand side of a list
     *
     * @param key The key of the list
     * @param args A list of items to be pushed onto the list
     *
     * @return The length of the list after the push operation
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) > 0)
    [[nodiscard]] inline int lpush(const std::string_view key, Args &&...args) & {
      auto replyPtr = push("LPUSH", key, std::forward<Args>(args)...);
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Pop an item or items from the left-hand side of a list
     *
     * @param key The key of the list
     * @param count How many items to pop (default 1)
     *
     * @return A `std::vector` of `std::string` or `std::nullopt` if no items were popped
     *
     */
    [[nodiscard]] inline std::optional<std::vector<std::string>> lpop(const std::string_view key,
                                                                      size_t count = 1) & {
      return pop("LPOP", key, count);
    }

    /**
     * @brief Pop an item or items from the right-hand side of a list
     *
     * @param key The key of the list
     * @param count How many items to pop (default 1)
     *
     * @return A `std::vector` of `std::string` or `std::nullopt` if no items were popped
     *
     */
    [[nodiscard]] inline std::optional<std::vector<std::string>> rpop(const std::string_view key,
                                                                      size_t count = 1) & {
      return pop("RPOP", key, count);
    }

    /**
     * @brief Remove elements from a list
     *
     * @param key The key of the list
     * @param count How many instances of element to remove
     * @param element The element to remove
     *
     * @return The numder of elements removed
     *
     */
    [[nodiscard]] inline int lrem(const std::string_view key, int count, const std::string_view element) & {
      auto cnt = std::to_string(count);
      auto argv = std::array<std::string_view, 4>{"LREM", key, cnt, element};
      auto replyPtr = commandArgv(argv);
      if (!intReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("LREM failed");
      }
      return replyPtr->integer;
    }

    /**
     * @brief Get the length of a list
     *
     * @param key The key of the list
     *
     * @return The number of items in the list
     *
     */
    [[nodiscard]] inline int llen(const std::string_view key) & {
      auto argv = std::array<std::string_view, 2>{"LLEN", key};
      auto replyPtr = commandArgv(argv);
      if (!intReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("LLEN failed");
      }
      return replyPtr->integer;
    }

    /**
     * @brief Get a range of values from a list
     *
     * @param key the list key
     * @param start the index of the first element in the range
     * @param stop the index of the last element in the range
     *
     * @return a vector of strings containing the list elements, or `std::nullopt` if none were found
     *
     */
    [[nodiscard]] inline std::optional<std::vector<std::string>> lrange(const std::string_view key, int start,
                                                                        int stop) & {
      auto stt = std::to_string(start);
      auto stp = std::to_string(stop);
      auto argv = std::array<std::string_view, 4>{"LRANGE", key, stt, stp};
      auto replyPtr = commandArgv(argv);
      if (!arrayLikeReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("LRANGE failed");
      }
      if (replyPtr->elements == 0) return std::nullopt;
      std::vector<std::string> itemVec;
      for (auto i{0u}; i < replyPtr->elements; i++) {
        auto *elem = replyPtr->element[i];
        itemVec.emplace_back(elem->str, elem->len);
      }
      return itemVec;
    }

    /**
     * @brief Publish a message to a subscription channel
     *
     * @param channel The name of the channel
     * @param message The message text
     *
     * @return The number of subscribers to which the message was published
     *
     */
    [[nodiscard]] inline int publish(const std::string_view channel, const std::string_view message) & {
      auto replyPtr = commandArgv({"PUBLISH", channel, message});
      if (replyPtr->type != REDIS_REPLY_INTEGER) return 0;
      return replyPtr->integer;
    }

    /**
     * @brief Add a listener to a queue and set a callback
     *
     * @param queueName The name of the queue to listen on
     * @param callback The callable object to process received messages
     *
     * @return A boolean value indicating whether the listener was successfully added
     *
     * @details This function adds a listener to a Redis queue (which is a list object). It launches a
     * thread which performa a blocking pop operation on the __left__ end of the queue. For FIFO queue
     * behaviour, items should be added to the queue with `RPUSH`. The `queueName` paraneter is a string
     * that identifies the queue for future reference. The `callback` parameter is a callable object (a
     * lambda, a struct or class implmenting `operator()` etc.) of type `ListenerCallback`. The callback
     * expects two parameters that are compatible with `std::string_view`, and returns nothing.
     *
     */
    bool addListener(const std::string queueName, ListenerCallback callback,
                     unsigned blockingTimeoutMillis = defaultBlockingTimeoutMillis) & {
      std::string timeoutStr;

      if (blockingTimeoutMillis < minBlockingTimeoutMillis) {
        timeoutStr = blockingNoTimeout;
      } else {
        timeoutStr =
            std::to_string(static_cast<double>(std::clamp(blockingTimeoutMillis, minBlockingTimeoutMillis,
                                                          maxBlockingTimeoutMillis)) /
                           1000.0);
      }

      auto lock = std::scoped_lock(listenerMutex);
      if (auto lthread = listeners.find(queueName); lthread != listeners.end() && lthread->second.joinable())
        return false;

      auto listener = [this, qn = queueName, ts = timeoutStr,
                       handler = std::move(callback)](std::stop_token tok) mutable {
        auto ctx = createContext(config);

        while (!tok.stop_requested()) {
          auto replyPtr = commandArgv(ctx.get(), {"BLPOP", qn, ts.c_str()});
          if (!tok.stop_requested() && bpopReplyOk(context.get(), replyPtr)) {
            auto *first = replyPtr->element[0];
            auto *second = replyPtr->element[1];
            auto item = std::make_pair(std::string_view{first->str, first->len},
                                       std::string_view{second->str, second->len});
            std::apply(handler, item);
          }
        }
      };

      auto [newThread, wasInserted] = listeners.try_emplace(queueName, listener);
      return wasInserted;
    }

    /**
     * @brief Remove a listener from a queue and terminate its thread
     *
     * @param queueName The name of the listener's queue
     *
     * @return A boolean value indicating whether the removal was successful
     *
     */
    bool removeListener(const std::string &queueName) & {
      auto lock = std::scoped_lock(listenerMutex);
      auto lthread = listeners.find(queueName);
      if (lthread == listeners.end()) return false;
      lthread->second.request_stop();
      return listeners.erase(queueName) > 0;
    }

    /**
     * @brief Subscribe to one or more channels and provide a callback to process received messages
     *
     * @param args A parameter pack of length N > 1, the first N - 1 of which are the names of channels
     * to which the client should subscribe, and the last being a callable object of type
     * `SubscriberCallback` that will receive the message
     *
     * @return an opaque 64-bit handle with which to reference the subscriber instance subsequently
     *
     * @details The `args` parameter is an arbitrary-length set of arguments representing a list of channels
     * followed by a callback to handle received messages: `<channel 1> <channel 2> ... <channel N>
     * <callback>`. The callnack parameter must be a callable object (free function, lambda etc.) that
     * conforms to the `SubscriberCallback` specification. A `SubscriberCallback` object takes a `std::span`
     * of `std::string_view` and returns nothing:\n
     * `    using SubscriberCallback = std::function<void(std::span<std::string_view>)>;`
     *
     */
    template <typename... Args>
      requires(sizeof...(Args) >= 2)
    [[nodiscard]] uint64_t subscribe(Args &&...args) & {
      auto argsTuple = std::forward_as_tuple(std::forward<Args>(args)...);
      constexpr std::size_t N{sizeof...(Args)};
      using argDecltype = decltype(argsTuple);

      using CallbackArg = std::tuple_element_t<N - 1, argDecltype>;
      static_assert(std::constructible_from<SubscriberCallback, std::decay_t<CallbackArg>>,
                    "subscribe: last argument must be a callable convertible to SubscriberCallback");
      SubscriberCallback cb{std::get<N - 1>(argsTuple)};

      auto allChannels = [&]<std::size_t... I>(std::index_sequence<I...>) -> std::vector<std::string_view> {
        static_assert(
            ((std::convertible_to<std::decay_t<std::tuple_element_t<I, argDecltype>>, std::string_view>) &&
             ...),
            "subscribe: channel args must be string_view-like");
        static_assert(
            (!(std::is_same_v<std::remove_cvref_t<std::tuple_element_t<I, argDecltype>>, std::string> &&
               std::is_rvalue_reference_v<std::tuple_element_t<I, argDecltype>>) &&
             ...),
            "subscribe: rvalue std::string not allowed (temporary, would dangle)");
        return std::vector<std::string_view>{std::string_view{std::get<I>(argsTuple)}...};
      }(std::make_index_sequence<N - 1>{});

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

      auto messageQueuePtr = std::make_shared<MessageQueue>();

      auto subPromise = std::make_shared<std::promise<void>>();
      auto finished = subPromise->get_future().share();

      auto subscriber = [this, chans = std::move(channels), patChans = std::move(patChannels),
                         handler = std::move(cb), subPromise,
                         mqPtr = messageQueuePtr](std::stop_token tok, Subscriber &sub) mutable {
        auto ctx = connectAndSubscribe(config, chans, patChans);

        auto contextFileDesc = ctx->fd;
        if ((mqPtr->signalFileDesc = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
          try {
            subPromise->set_value();
          } catch (...) {
          }
          return;
        }

        pollfd pfds[2] = {{contextFileDesc, POLLIN, 0}, {mqPtr->signalFileDesc, POLLIN, 0}};

        {
          std::stop_callback onRequestStop(tok, [mqPtr] {
            mqPtr->terminate.store(true, std::memory_order_release);
            mqPtr->wakeup();
          });

          bool done{false};

          auto drain = [](int fd) {
            uint64_t tmp;
            (void)::read(fd, &tmp, sizeof tmp);
          };

          sub.ready.count_down();

          while (!done) {
            if (mqPtr->terminate.load(std::memory_order_acquire)) {
              done = true;
              break;
            }

            int pollResult;
            do {
              pollResult = ::poll(pfds, 2, -1);  // block; eventfd wakes us
            } while (pollResult < 0 && errno == EINTR);

            if (pollResult < 0) {
              if (errno == EINTR) continue;
              break;
            }

            if (pfds[1].revents & POLLIN) {
              mqPtr->drain();
              std::deque<Message> messages;
              {
                auto mLock = std::scoped_lock(mqPtr->queueMutex);
                messages.swap(mqPtr->messages);
              }
              while (!messages.empty()) {
                const auto &msg = messages.front();
                auto msgChannels = std::move(msg.channels);
                switch (msg.command) {
                  case Command::SUBSCRIBE: {
                    msgChannels.insert(msgChannels.begin(), "SUBSCRIBE");
                    (void)appendCommandArgv(ctx.get(), msgChannels);
                    (void)flushPending(ctx.get());
                    break;
                  }

                  case Command::PSUBSCRIBE: {
                    msgChannels.insert(msgChannels.begin(), "PSUBSCRIBE");
                    (void)appendCommandArgv(ctx.get(), msgChannels);
                    (void)flushPending(ctx.get());
                    break;
                  }

                  case Command::UNSUBSCRIBE: {
                    msgChannels.insert(msgChannels.begin(), "UNSUBSCRIBE");
                    (void)appendCommandArgv(ctx.get(), msgChannels);
                    (void)flushPending(ctx.get());
                    break;
                  }

                  case Command::PUNSUBSCRIBE: {
                    msgChannels.insert(msgChannels.begin(), "PUNSUBSCRIBE");
                    (void)appendCommandArgv(ctx.get(), msgChannels);
                    (void)flushPending(ctx.get());
                    break;
                  }

                  case Command::UNSUBSCRIBEALL: {
                    msgChannels.insert(msgChannels.begin(), "UNSUBSCRIBE");
                    (void)appendCommandArgv(ctx.get(), msgChannels);
                    (void)flushPending(ctx.get());
                    msgChannels.at(0) = "PUNSUBSCRIBE";
                    (void)appendCommandArgv(ctx.get(), msgChannels);
                    (void)flushPending(ctx.get());
                    break;
                  }

                  case Command::TERMINATE: {
                    mqPtr->terminate.store(true, std::memory_order_release);
                    done = true;
                    break;
                  }
                }
                messages.pop_front();
              }
            }

            if (done) break;

            // socket died: attempt to reccreate context with exponential backoff
            if (pfds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
              drain(pfds[0].fd);
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
        (void)::close(mqPtr->signalFileDesc);
        try {
          subPromise->set_value();
        } catch (...) {
        }
      };  // end subscriber lambda

      auto subHash = [this]() -> uint64_t {
        uint64_t thash{0xcbf29ce484222325ULL};
        uint64_t k = std::hash<uint64_t>{}(idGen());
        thash ^= k + 0x9e3779b97f4a7c15ULL + (thash << 6) + (thash >> 2);
        k = std::hash<uint64_t>{}(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
        return thash ^ (k + 0x9e3779b97f4a7c15ULL + (thash << 6) + (thash >> 2));
      }();

      {
        auto subLock = std::scoped_lock(subscriberMutex);
        auto [iter, inserted] = subscriberMap.emplace(
            std::piecewise_construct, std::forward_as_tuple(subHash), std::forward_as_tuple());
        auto &sub = iter->second;
        sub.finished = finished;
        sub.queuePtr = std::move(messageQueuePtr);
        sub.subThread = std::jthread(std::move(subscriber), std::ref(sub));
        sub.ready.wait();
      }

      return subHash;
    }  // end subscribe(channels..., cb)

    /**
     * @brief Tell an existing subscriber to subscribe to additional channels
     *
     * @param subId the subscriber handle as returned from the initial call to `subscribe()`
     * @param args a parameter pack containing the names of the additional subscription channels
     *
     * @return a boolean value indicating whether the operation was successful
     *
     */
    template <satisfies_stringview... Args>
      requires(sizeof...(Args) >= 1)
    [[nodiscard]] bool subscribe(uint64_t subId, Args &&...args) & {
      auto subscriberEntry = subscriberMap.find(subId);
      if (subscriberEntry == subscriberMap.end()) return false;

      constexpr std::size_t N{sizeof...(Args)};
      auto allChannels = std::array<std::string_view, N>{std::string_view(std::forward<Args>(args))...};

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
        return std::make_pair(chans, patChans);
      }(allChannels);

      auto &sub = subscriberEntry->second;
      sub.ready.wait();
      {
        auto qLock = std::scoped_lock(sub.queuePtr->queueMutex);
        if (!channels.empty()) {
          sub.queuePtr->messages.push_back(Message{Command::SUBSCRIBE, std::move(channels)});
        }
        if (!patChannels.empty()) {
          sub.queuePtr->messages.push_back(Message{Command::PSUBSCRIBE, std::move(patChannels)});
        }
      }
      sub.queuePtr->wakeup();

      // give Redis some time to adjust its internal pubsub configuration
      std::this_thread::sleep_for(25ms);

      return true;
    }  // end subscribe(id, channels...)

    /**
     * @brief Tell a subscriber to unsubscribe from some channels
     *
     * @param subId the subscriber handle as returned from the initial call to `subscribe()`
     * @param args a parameter pack containing the names of the channels that are to be unsubscribed
     *
     * @return A boolean value inicating whether the operation was successful
     *
     */
    template <satisfies_stringview... Args>
    [[nodiscard]] bool unsubscribe(uint64_t subId, Args &&...args) & {
      auto subscriberEntry = subscriberMap.find(subId);
      if (subscriberEntry == subscriberMap.end()) return false;

      constexpr std::size_t N{sizeof...(Args)};
      auto allChannels = std::array<std::string_view, N>{std::string_view(std::forward<Args>(args))...};

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
        return std::make_pair(chans, patChans);
      }(allChannels);

      auto &sub = subscriberEntry->second;
      sub.ready.wait();
      {
        auto qLock = std::scoped_lock(sub.queuePtr->queueMutex);
        if (channels.empty() && patChannels.empty()) {
          sub.queuePtr->messages.push_back(Message{Command::UNSUBSCRIBEALL, {}});
        }
        if (!channels.empty()) {
          sub.queuePtr->messages.push_back(Message{Command::UNSUBSCRIBE, std::move(channels)});
        }
        if (!patChannels.empty()) {
          sub.queuePtr->messages.push_back(Message{Command::PUNSUBSCRIBE, std::move(patChannels)});
        }
      }
      sub.queuePtr->wakeup();

      // give Redis some time to adjust its internal pubsub configuration
      std::this_thread::sleep_for(25ms);

      return true;
    }  // end unsubscribe()

    /**
     * @brief Stop a running subscriber instance
     *
     * @param subId The subscriber instance's handle passed back from the first call to `subscribe`
     *
     * @details This function halts a rumming subscriber, cleans up its entry in the lookup table, and
     * terminates thread execution. At this point the handle becomes invalid and should not be used again.
     *
     */
    void stop(uint64_t subId) {
      auto subscriberEntry = subscriberMap.find(subId);

      if (subscriberEntry != subscriberMap.end()) {
        auto &sub = subscriberEntry->second;
        {
          auto qLock = std::scoped_lock(sub.queuePtr->queueMutex);
          sub.queuePtr->messages.push_back(Message{Command::TERMINATE, {}});
        }
        sub.queuePtr->wakeup();
      }
    }

    /**
     * @brief Load a Lua script file into the script cache and return its SHA1 identifier
     *
     * @param scriptName The name under which to store the script identifier in the lookup table
     * @param scriptFilePath The path to the script file
     *
     * @return A `std::optional` value containing the SHA1 hash of the script if loading was successful
     *
     */
    [[nodiscard]] std::optional<std::string> loadScriptFromFile(const std::string &scriptName,
                                                                const fs::path &scriptFilePath) {
      auto filepath = fs::weakly_canonical(fs::absolute(scriptFilePath));
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
      }

      auto script = std::string_view{std::bit_cast<char *>(inbuffer.get()), fsize};

      return loadScript(scriptName, script);
    }

    /**
     * @brief Load a string containing a Lua script into the script cache and return its SHA1 identifier
     *
     * @param scriptName The name under which to store the script identifier in the lookup table
     * @param script The text of the Lua script
     *
     * @return A `std::optional` value containing the SHA1 hash of the script if loading was successful
     *
     */
    [[nodiscard]] std::optional<std::string> loadScript(const std::string &scriptName,
                                                        const std::string_view script) & {
      auto replyPtr = commandArgv(context.get(), {"SCRIPT", "LOAD", script});

      if (stringReplyOk(context.get(), replyPtr)) {
        auto shaStr = std::string(replyPtr->str, replyPtr->len);
        (void)scriptHashes.insert_or_assign(scriptName, shaStr);
        return std::string(replyPtr->str, replyPtr->len);
      }

      return std::nullopt;
    }

    /**
     * @brief Evaluate a Lue script given its name in the lookup table
     *
     * @param scriptName The name of the script in the lookup table
     * @param numKeys The number of entries in `args` that are keys
     * @param args A parameter pack of keys and arguments to be passed to the Lua script
     *
     * @return The result of the evaluation
     *
     */
    template <satisfies_stringview... Args>
    [[nodiscard]] ReplyPointer evaluate(const std::string &scriptName, size_t numKeys, Args &&...args) & {
      auto scriptHash = scriptHashes.find(scriptName);
      if (scriptHash == scriptHashes.end()) return ReplyPointer{};
      return evaluateBySha(scriptHash->second, numKeys, std::forward<Args>(args)...);
    }

    /**
     * @brief Evaluate a Lue script given its SHA1 hash
     *
     * @param scriptName The SHA1 hash of the script
     * @param numKeys The number of entries in `args` that are keys
     * @param args A parameter pack of keys and arguments to be passed to the Lua script
     *
     * @return The result of the evaluation
     *
     */
    template <satisfies_stringview... Args>
    [[nodiscard]] ReplyPointer evaluateBySha(const std::string &sha, size_t numKeys, Args &&...args) & {
      constexpr size_t N = 3 + sizeof...(Args);
      auto nk = std::to_string(numKeys);
      auto argv =
          std::array<std::string_view, N>{"EVALSHA", sha, nk, std::string_view(std::forward<Args>(args))...};
      return commandArgv(argv);
    }

    /**
     * @brief Flush all scripts from the script cache and cleat the lookup table
     *
     * @param async Flush the scripts asynchronously
     *
     * @return A boolean value inicating whether the operation was successful
     *
     */
    [[nodiscard]] inline bool flushScripts(bool async = false) & {
      scriptHashes.clear();
      auto flushArgs = async ? std::vector<std::string_view>{"SCRIPT", "FLUSH", "ASYNC"}
                             : std::vector<std::string_view>{"SCRIPT", "FLUSH"};
      auto replyPtr = commandArgv(context.get(), flushArgs);
      return replyPtr->type == REDIS_REPLY_STATUS && replyPtr->str == "OK"s;
    }

    /**
     * @brief Determine whether a script with the given name exists in the script cache
     *
     * @param name The name of the script in the lookup table
     *
     * @return A boolean value indicating whether the script exists
     *
     */
    [[nodiscard]] inline bool scriptExists(const std::string &name) & {
      auto hash = scriptHashes.find(name);
      if (hash == scriptHashes.end()) return false;
      auto argv = std::array<std::string_view, 3>{"SCRIPT", "EXISTS", hash->second};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type != REDIS_REPLY_ARRAY || replyPtr->elements != 1 ||
          replyPtr->element[0]->type != REDIS_REPLY_INTEGER || replyPtr->element[0]->integer != 1) {
        scriptHashes.erase(name);
        return false;
      }
      return true;
    }

    /**
     * @brief Determine whether a script or scripts with the given SHA1 hash exist in the script cache
     *
     * @param args A list of the SHA1 hashes of the scripts whose existence is to be ascertained
     *
     * @return A vector of booleans corresponding to each of the hashes indicating whether it exists
     *
     */
    template <satisfies_stringview... Args>
    [[nodiscard]] inline std::vector<bool> scriptsExistBySha(Args &&...args) & {
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv =
          std::array<std::string_view, N>{"SCRIPT", "EXISTS", std::string_view(std::forward<Args>(args))...};
      auto replyPtr = commandArgv(argv);
      std::vector<bool> existsVec;
      if (replyPtr->type == REDIS_REPLY_ARRAY && replyPtr->elements > 0) {
        existsVec.reserve(replyPtr->elements);
        for (auto i{0u}; i < replyPtr->elements; i++) {
          existsVec.push_back(static_cast<bool>(replyPtr->element[i]->integer));
        }
      }

      return existsVec;
    }

    /*************************************************************************************** */
    /**************************************** PRIvATE ****************************************/
    /*************************************************************************************** */

   private:
    Config config;
    ContextPointer context;
    mutable std::mutex listenerMutex;
    ListenerMap listeners;
    mutable std::mutex subscriberMutex;
    SubscriberMap subscriberMap;
    RandomGenerator idGen;
    ScriptMap scriptHashes;
    std::jthread reaperThread;

    template <typename... Args>
    [[nodiscard]] inline ReplyPointer command(redisContext *ctx, const char *format, Args &&...args) & {
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

#if !HAS_SPAN_INIT_LIST_CTOR
    [[nodiscard]] inline ReplyPointer commandArgv(redisContext *ctx,
                                                  std::initializer_list<const std::string_view> args) {
      return commandArgv(ctx, std::span<const std::string_view>(args.begin(), args.size()));
    }
#endif

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
      int done{0};
      while (!done) {
        auto rc = ::redisBufferWrite(ctx, &done);
        if (rc == REDIS_ERR) return false;
      }
      return true;
    }

    [[nodiscard]] inline ReplyPointer getReply(redisContext *ctx) {
      redisReply *reply;
      auto result = ::redisGetReply(ctx, std::bit_cast<void **>(&reply));
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
      if (allowResp3 && cfg.useResp3) {
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

    template <satisfies_stringview... Args>
      requires(sizeof...(Args) > 0)
    [[nodiscard]] inline ReplyPointer push(const std::string_view dir, const std::string_view key,
                                           Args &&...args) {
      constexpr size_t N = 2 + sizeof...(Args);
      auto argv = std::array<std::string_view, N>{dir, key, std::string_view(std::forward<Args>(args))...};
      return commandArgv(argv);
    }

    [[nodiscard]] inline std::optional<std::vector<std::string>> pop(const std::string_view dir,
                                                                     const std::string_view key,
                                                                     size_t count) {
      auto cnt = std::to_string(count);
      auto argv = std::array<std::string_view, 3>{dir, key, cnt};
      auto replyPtr = commandArgv(argv);
      if (replyPtr->type == REDIS_REPLY_NIL) return std::nullopt;
      if (!arrayLikeReplyOk(context.get(), replyPtr)) {
        throw std::runtime_error("POP failed");
      }
      std::vector<std::string> itemVec;
      itemVec.reserve(replyPtr->elements);
      for (auto i{0u}; i < replyPtr->elements; i++) {
        auto *elem = replyPtr->element[i];
        itemVec.emplace_back(elem->str, elem->len);
      }
      return itemVec;
    }

    void handleSubscriptionMessage(redisContext *ctx, SubscriberCallback &&handler) {
      void *reply{nullptr};
      if (::redisGetReply(ctx, &reply) == REDIS_OK) {
        auto replyPtr = ReplyPointer(static_cast<redisReply *>(reply));
        auto elements = extractMessageElements(replyPtr);
        handler(elements);
      }

      // drain any further messages
      while (true) {
        reply = nullptr;
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
          elementVec.emplace_back(elems[i]->str, elems[i]->len);
        }
      }

      return elementVec;
    }

    void reapSubscribers() {
      auto subLock = std::scoped_lock(subscriberMutex);
      std::erase_if(subscriberMap, [](auto &subscriberEntry) {
        auto &[id, sub] = subscriberEntry;
        if (sub.finished.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
          if (sub.subThread.joinable()) sub.subThread.join();
          return true;  // erase
        }
        return false;
      });
    }

    void startReaper() {
      const auto period = 2s;
      reaperThread = std::jthread([this, period](std::stop_token tok) {
        std::mutex reaperMutex;
        std::condition_variable timerVar;
        bool stop = false;

        std::stop_callback stopReaper(tok, [&] {
          auto reapLock = std::scoped_lock(reaperMutex);
          stop = true;
          timerVar.notify_all();
        });

        auto next = std::chrono::steady_clock::now() + period;

        while (true) {
          auto waitLock = std::unique_lock(reaperMutex);
          // Wake on stop or at the next tick
          if (timerVar.wait_until(waitLock, next, [&] { return stop; })) break;  // stopped
          waitLock.unlock();

          reapSubscribers();

          do {
            next += period;
          } while (next <= std::chrono::steady_clock::now());
        }
      });
    }

    inline bool replyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err);
    }

    inline bool replyOk(redisContext *ctx, ReplyPointer const &replyPtr, int expectedType) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == expectedType);
    }

    inline bool intReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_INTEGER);
    }

    inline bool stringReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_STRING);
    }

    inline bool statusReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_STATUS &&
              replyPtr->len == 2 && !std::strncmp(replyPtr->str, "OK", 2));
    }

    inline bool arrayLikeReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err &&
              (replyPtr->type == REDIS_REPLY_MAP || replyPtr->type == REDIS_REPLY_ARRAY) &&
              (replyPtr->type == REDIS_REPLY_MAP ? replyPtr->len % 2 == 0 : true));
    }

    inline bool setReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_SET);
    }

    inline bool subscribeReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_ARRAY &&
              replyPtr->elements == 3);
    }

    inline bool bpopReplyOk(redisContext *ctx, ReplyPointer const &replyPtr) noexcept {
      return (replyPtr != nullptr && !ctx->err && replyPtr->type == REDIS_REPLY_ARRAY &&
              replyPtr->elements == 2);
    }

#if REDISCPP_DEBUG
    std::string dumpReply(const ReplyPointer &replyPtr) {
      std::ostringstream out;
      dumpReplyR(out, replyPtr.get());

      return out.str();
    }

    void dumpReplyR(std::ostringstream &out, const redisReply *reply, unsigned depth = 0) {
      std::string indent;
      for (auto ix{0u}; ix < depth; ix++) {
        indent += "      ";
      }
      if (reply == nullptr) {
        out << indent << "nullptr";
      } else {
        constexpr auto types = std::array<std::string_view, 15>{
            "",     "STRING", "ARRAY", "INTEGER", "NIL",  "STATUS", "ERROR", "DOUBLE",
            "BOOL", "MAP",    "SET",   "ATTR",    "PUSH", "BIGNUM", "VERB"};
        auto type = reply->type;
        out << indent << "type: " << types[type] << "\n";
        if (type == REDIS_REPLY_INTEGER) {
          out << indent << "integer: " << reply->integer << "\n";
        }
        if (type == REDIS_REPLY_DOUBLE) {
          out << indent << "integer: " << reply->dval << "\n";
        }
        if (type == REDIS_REPLY_BOOL) {
          out << indent << "bool: " << (reply->integer ? "true" : "false") << "\n";
        }
        if (type == REDIS_REPLY_ERROR || type == REDIS_REPLY_STRING || type == REDIS_REPLY_STATUS ||
            type == REDIS_REPLY_DOUBLE || type == REDIS_REPLY_BIGNUM || type == REDIS_REPLY_VERB) {
          out << indent << "len: " << reply->len << "\n"
              << indent << "str: " << (reply->str == nullptr ? "nullptr" : reply->str) << "\n";
        }
        if (type == REDIS_REPLY_VERB) {
          out << indent << "vtype: " << reply->vtype << "\n";
        }
        if (type == REDIS_REPLY_ARRAY || type == REDIS_REPLY_SET || type == REDIS_REPLY_MAP) {
          out << indent << "elements: " << reply->elements << "\n";
          for (auto i{0u}; i < reply->elements; i++) {
            out << indent << "  [" << i << "]:\n";
            dumpReplyR(out, reply->element[i], depth + 1);
          }
        }
      }
    }
#else
    std::string dumpReply(const ReplyPointer &replyPtr) const {
      throw std::runtime_error("dumpReply is not implemented in Release varsion");
    }
#endif

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
        struct sigaction sigAct{};
        sigAct.sa_flags = 0;
        sigemptyset(&sigAct.sa_mask);
        sigAct.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &sigAct, nullptr);
      });
    }
  };  // end class Client
};  // namespace RedisCpp
