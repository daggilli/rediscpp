#include <format>
#include <print>
#include <ranges>
#include <shared_mutex>
#include <thread>

#include "rediscppconfig.hpp"
#include "rediscppimpl.hpp"

using namespace std::chrono_literals;

/** A subscription listener class that takes subscription messages and prints them.
 *
 */
struct SubscriptionHandler {
  SubscriptionHandler(const std::string &name_) : name(name_) {}
  void operator()(std::span<std::string_view> elements) {
    std::print("Subscriber message [{}]: --|", name);
    std::string sep{""};
    for (auto &elem : elements) {
      std::print("{}{}", sep, elem);
      sep = " ";
    }
    std::println("|--");
  }
  std::string name;
};

int main() {
  RedisCpp::Config config("localhost", 6379, 6);
  RedisCpp::Client client(config);

  // listeners for the subscriptions
  SubscriptionHandler handlera("thing1"), handlerb("thing2");

  /**
   * Subscribe to a list of channels. More than one subscriber can listen to a given chaneel. A Client can
   * have more than one active subscriber object. The return value is an opaque 64-bit integer which should be
   * used to refer to a specific subscriber object in future operations. The subscription handlers will
   * immeidately issue messages condirming their subscriptions.
   *
   */
  auto suba = client.subscribe("testchan", "secondchan", "sharedchan", "wildcard*", handlera);
  auto subb = client.subscribe("sharedchan", "also", handlerb);

  std::println("suba: {:016X}\nsuba: {:016X}", suba, subb);

  // publish a message
  auto pub = client.publish("testchan", "first message");
  std::println("Num subscribers: {}", pub);

  /**
   * Publish a message on a channel both subscribers are listening to. Message arrival at the handlers is
   * asynchronous. There is no guarantee that messages will arrive in any order, or even that they will not
   * arrive interleaved.
   *
   */
  pub = client.publish("sharedchan", "a message to both");
  std::println("Num subscribers: {}", pub);

  // pattern channels take a wildcard name
  pub = client.publish("wildcard1", "a pattern message");
  std::println("Num subscribers: {}", pub);

  /**
   * Channels cxn be added to a subscriber by passing the opaque handle returned in the original call to
   * subscribe()
   */
  auto added = client.subscribe(suba, "newchan", "foochan*");
  std::println("added {}", added);

  /**
   * A subscription to a channel can be dropped by passing the subscriber's opaque handle
   *
   */
  (void)client.unsubscribe(suba, "testchan");

  // give the Redis server some time to adjust its internal configuration
  // std::this_thread::sleep_for(25ms);

  // messages for 'testchan' no longer appear
  pub = client.publish("testchan", "a message after unsubscribe");
  std::println("Num subscribers: {}", pub);

  /**
   * A subscriber can be stopped completely. It will no longer receive any messages. The opaque handle should
   * not be used after this point.
   *
   */
  client.stop(subb);

  // messages on the 'also' channel are lost
  pub = client.publish("also", "message after stop");
  std::println("Num subscribers: {}", pub);

  std::println("Exit in 2.5s");
  std::this_thread::sleep_for(2500ms);

  return 0;
}
