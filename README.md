# REDISCPP

## A simple, modern C++ wrapper for Redis

#### RedisCpp is a header-only interface to the Redis in-memory key/value store.

The goal of this project is to present a C++ interface to Redis that is as simple as possible to use. It uses modern C++ features to perform a number of checks at compile time, making correct usage easier. Binaries built using a release-grade configuration are typically only a few hundred kilobytes, making them more suitable for small computers with restricted memory such as the Raspberry Pi.

### Prerequisites

A modern C++ compiler is required. The minimum standard that will compile cleanly is C++23. C++26 is preferred. In practice this means GCC 15 or later, or clang 20 or later. These are heavyweight compilers, so for use on small computers cross-compilation may be the best strategy.

RedisCpp is built on top of [hiredis](https://github.com/redis/hiredis). The hiredis library and development headers must ne installed.

Parsing the configuration file uses [jsoncpp](https://github.com/open-source-parsers/jsoncpp) and this must be installed along with tis header files.

If cross-compiling, the target machine will need the hiredis and jsoncpp libraries.

Building the documentation requires [Doxygen](https://doxygen.nl/index.html).

### Installation

Navigate to the project root. The included `CMAKE` script will prepare the necessary makefiles. Then `INSTALL` will copy the required files to `/usr/local/bin`. Package configuration files for `cmake` will be written to `/usr/local/share/RedisCpp/cmake` so the libary can be located with `find_package` in your project's `CMakeLists.txt` file. To build the examples, use the `DBUILD` and `RBUILD` scripts to create the Debug and Release versions respectively. The documentation can be generated with the `DOCS` script (assuming you have Doxygen installed).

### Usage

The library is header-only. By default it is installed in `/usr/local/bin`. Typically, simply `#include <rediscpp.hpp>`. This includes two subheaders, `rediscppimpl.hpp` and `rediscppconfig.hpp` which can be included individually in your project if you do not need the functionality of one of them.

### Example code

Almnost all of the functionality of the library is covered in a set of example programs which are found in `src/examples`. Thet can be built with the `RBUILD` and `DBUILD` scripts for Release and Debug versions respectively. The Debug versions are built with Address Sanitizer enabled, which helps to catch a wide range of memory errors such as use-after-free or dereferending `nullptr`. Executables will be created in `build/examples/[Release|Debug]`. The core class through which communication with the Redis server is mediated is `RedisCpp::Client`. Configuration is handled by the `RedisCpp:Config` class.

Many of the available functions take a variable number of arguments, which in general must be string-like. This means C-style strings, `std::string` and `std::string)view`. In most cases, rvalue `std::string` paraneters are _not_ allowed. In other words code that looks like:

```cpp
  client.func(std::string("abc"), ...);
```

will fail to compile. The reason for this is that internally the code uses `std::string_view` for string-like objects, and allocating a temporary like this is dangerous as it will be immediately deallocated, leaving the internal `string_view` pointing to invalid memory. This constraint is enforced at compile time, making it safer to use the library.

On the other hand, code such as:

```cpp
  client.func(std::string_view("abc"), ...);
```

is perfectly acceptable (albeit fairly useless) as `std::string_view` does not take ownership of the `"abc"` string literal.

Most of the public member functions of the the `RedisCpp::Client` class are ref-qualified `&`. This is to prevent the use of temporaries. In other words, code such as the following will fail to compile:

```cpp
RedisCpp::Client(RedisCpp::Config{"localhost", 6379}).get("foo");
```

This is intentional.

### Client configuration

An instance of `RedisCpp::Client` requires a configuration of type `RedisCpp::Config`. This must at least specify the server URL and port number.

A constructor for `RedisCpp::Config` exists to specify parameters individually, which takes mandatory values for the server host and port. Additional parameters control the database number to use (0-15), whether to use basic AUTH to connect, the username and password if so, and whether to upgrade the protocol to the RESP3 standard.

The `RedisCpp::Config` class has the following shape:

```cpp
std::string hostname;
int port;
std::optional<int> db;
bool useAuth;
std::optional<std::string> username;
std::optional<std::string> password;
std::optional<bool> useResp3;

```

A client configuration can be created by providing the path to a TOML file containing a suitable object specification. The TOML object in this files should have the following shape:

```toml
[redis]
hostname = <string>
port = <int>
db = <int 0-15, optional, default 0>
useauth = <boolen, optional, default false>
username = <string, optional, default null>
password = <string, optional, default null>
useresp3 = <boolean, optional, default true>
```

Thus a possibel config file could looke like:

```toml
[redis]
hostname = "redis-server"
port = 26379
db = 12
useauth = true
username = "redisuser"
password = "jWhbkZmt"
```

Note that key names in this object are all **lowercase**.

A typical preamble for a program using this library might be as follows:

```cpp
#include <rediscpp.h>

int main() {
  RedisCpp::Config config("path/to/config");
  RedisCpp::Client client(config);

  ... // do something

  return 0;
}
```

Note that the connection to Redis is closed when the `Client` object goes out of scope. If a persistent connection is required it must be kept alive in some fashion (this is left to the user).

---

<sup><sub>
Copyright (c) 2025–2026 David Gillies<br>
SPDX-License-Identifier: BSD-3-Clause
<sub><sup>
