#include <cstdint>
#include <print>
#include <thread>

#include "redisconfig.hpp"
#include "rediscpp.hpp"

using namespace std::string_literals;
using namespace std::chrono_literals;

struct QueueHandler {
  void operator()(const std::string_view qname, const std::string_view value) {
    std::println("QHAND {} {}", qname, value);
  }
};

int main() {
  RedisCpp::Config cfg("config/redisconfig.json");
  auto client = RedisCpp::Client(cfg);

  QueueHandler qh;
  auto added = client.addListener("testq", qh);
  std::println("ADDED {}", added);

  auto newget = client.get("foox");
  if (newget.has_value()) {
    std::println("ASYNC OPTGET {}", newget.value());
  }

  auto ix = 0;
  while (ix++ < 6) {
    std::this_thread::sleep_for(1s);
  }

  return 0;
}
