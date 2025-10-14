cmake_minimum_required(VERSION 3.28)
project(RedisCpp LANGUAGES CXX VERSION 1.0.0)
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

find_package(randomint)

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

target_compile_definitions(${EXE} PRIVATE
    $<$<CONFIG:Debug>:REDISCPP_DEBUG=1>
    $<$<CONFIG:Release>:REDISCPP_RELEASE=1>
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

include(GNUInstallDirs)

install(TARGETS ${PROJECT_NAME}
  EXPORT ${PROJECT_NAME}_Targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

include(CMakePackageConfigHelpers)
write_basic_package_version_file("${PROJECT_NAME}ConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

configure_package_config_file(
  "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
  "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)

install(EXPORT ${PROJECT_NAME}_Targets
  FILE ${PROJECT_NAME}Targets.cmake
  NAMESPACE ${PROJECT_NAME}::
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)

install(FILES
  "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
  "${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/ DESTINATION include)
