cmake_minimum_required(VERSION 3.28)
project(RedisCpp LANGUAGES CXX)
set(CMAKE_VERBOSE_MAKEFILE ON)

set(EXE rediscpptest)

if(CMAKE_GENERATOR MATCHES "Ninja Multi-Config")
  set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "" FORCE)
  set(CMAKE_DEFAULT_BUILD_TYPE "Release" CACHE STRING "" FORCE)
endif()

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wunused -Wnrvo)

set(CMAKE_BUILD_WITH_INSTALL_RPATH ON)
set(CMAKE_INSTALL_RPATH "/usr/local/gcc-15/lib64")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Suppress warnings about deprecated is_trivial_v in C++26
if(CMAKE_CXX_STANDARD GREATER_EQUAL 26)
    add_compile_options(-Wno-deprecated)
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/app/$<CONFIG>")
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)

list(PREPEND CMAKE_PREFIX_PATH "/usr/local/lib/cmake/hiredis")
find_package(hiredis)

list(PREPEND CMAKE_PREFIX_PATH "/usr/local/jsoncpp-1.9.7")
find_package(jsoncpp CONFIG REQUIRED)

add_executable(${EXE} app/${EXE}.cpp)

add_library(${PROJECT_NAME} INTERFACE)

target_include_directories(
  ${PROJECT_NAME}
  INTERFACE
  $<BUILD_INTERFACE:${${PROJECT_NAME}_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_compile_options(${EXE} PRIVATE
  $<$<CONFIG:Debug>:-g -O1 -fno-omit-frame-pointer>
  $<$<CONFIG:Release>: -O4 -DNODEBUG -ffunction-sections -fdata-sections -fno-plt>
  $<$<AND:$<CONFIG:Debug>,$<BOOL:${ENABLE_ASAN}>>:-fsanitize=address>
)

target_link_options(${EXE} PRIVATE
  $<$<CONFIG:Release>: -Wl,--gc-sections -Wl,--as-needed>
  $<$<AND:$<CONFIG:Debug>,$<BOOL:${ENABLE_ASAN}>>:-fsanitize=address>
)

target_precompile_headers(${EXE} PRIVATE <format> <print> <json/value.h> <json/reader.h>)

if (TARGET jsoncpp_lib)
  set_property(TARGET jsoncpp_lib PROPERTY MAP_IMPORTED_CONFIG_DEBUG Release)
  set_property(TARGET jsoncpp_lib PROPERTY MAP_IMPORTED_CONFIG_RELWITHDEBINFO Release)
  target_link_libraries(${EXE} PRIVATE jsoncpp_lib)
endif ()

target_link_libraries(${EXE} PRIVATE ${PROJECT_NAME})
target_link_libraries(${EXE} PRIVATE hiredis)

get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
    message(STATUS "${_variableName}=${${_variableName}}")
endforeach()