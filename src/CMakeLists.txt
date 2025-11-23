cmake_minimum_required(VERSION 3.28)
project(RedisCpp LANGUAGES CXX VERSION 1.0.0)
set(CMAKE_VERBOSE_MAKEFILE ON)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  set(CUSTOM_LIBSTDCPP_DIR "/usr/local/lib64")

  # Ensure runtime finds the right libstdc++
  set(CMAKE_SKIP_BUILD_RPATH FALSE)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

  set(CMAKE_BUILD_RPATH "${CUSTOM_LIBSTDCPP_DIR}")
  set(CMAKE_INSTALL_RPATH "${CUSTOM_LIBSTDCPP_DIR}")
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

if(CMAKE_GENERATOR MATCHES "Ninja Multi-Config")
  set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "" FORCE)
  set(CMAKE_DEFAULT_BUILD_TYPE "Release" CACHE STRING "" FORCE)
endif()

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wunused -Wnrvo)

# Suppress warnings about deprecated is_trivial_v in C++26
if(CMAKE_CXX_STANDARD GREATER_EQUAL 26)
  add_compile_options(-Wno-deprecated)
endif()

option(BUILD_EXAMPLES "Build example executables" OFF)
option(BUILD_DOCS "Create documentation (requires Doxygen)" OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/examples/$<CONFIG>")
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)

list(PREPEND CMAKE_PREFIX_PATH "/usr/local/lib/cmake/hiredis")
find_package(hiredis QUIET CONFIG)

find_package(tomlplusplus REQUIRED)

find_package(PkgConfig REQUIRED)

if(NOT hiredis_FOUND)
  message(STATUS "hiredis CMake package not found, falling back to pkg-config")
  pkg_check_modules(HIREDIS REQUIRED hiredis)

  add_library(hiredis::hiredis INTERFACE IMPORTED)

  set_target_properties(hiredis::hiredis PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${HIREDIS_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES      "${HIREDIS_LIBRARIES}"
  )
endif()

add_library(${PROJECT_NAME} INTERFACE)

target_include_directories(
  ${PROJECT_NAME}
  INTERFACE
  $<BUILD_INTERFACE:${${PROJECT_NAME}_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# Include this project's headers if not under active development
# target_precompile_headers(${PROJECT_NAME} INTERFACE
#   <format> <print>
#   "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/rediscppimpl.hpp>"
#   "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/rediscppconfig.hpp>"
# )

target_precompile_headers(${PROJECT_NAME} INTERFACE
  <format> <print> <toml++/toml.hpp>
)

list(APPEND Examples
  client  
  config
  getsetdel
  hashmap
  list
  listener
  produceconsume
  pubsub
  script
  set
)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  set(ENABLE_ASAN OFF CACHE BOOL "Enable AddressSanitizer (Debug builds)" FORCE)
endif()

foreach(EXE IN LISTS Examples)
  add_executable(${EXE} examples/${EXE}.cpp)

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


  target_link_libraries(${EXE} PRIVATE ${PROJECT_NAME})
  target_link_libraries(${EXE} PRIVATE hiredis)

  set_property(TARGET ${EXE} PROPERTY EXCLUDE_FROM_ALL TRUE)
endforeach()

add_custom_target(examples DEPENDS ${Examples})

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

find_package(Doxygen REQUIRED)
get_filename_component(PROJECT_ROOT "${CMAKE_SOURCE_DIR}" DIRECTORY)
set(DOCS_OUT "${PROJECT_ROOT}/docs")
set(DOCS_INPUT "${PROJECT_ROOT}/src")

configure_file(
  ${CMAKE_SOURCE_DIR}/doxygen/Doxyfile.in
  ${CMAKE_BINARY_DIR}/Doxyfile
  @ONLY
)

add_custom_target(docs
  COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_BINARY_DIR}/Doxyfile
  COMMENT "Generating documentation in ${DOCS_OUT}"
  VERBATIM
)
