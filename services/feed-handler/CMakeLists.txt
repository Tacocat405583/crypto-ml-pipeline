cmake_minimum_required(VERSION 3.20)
project(feed_handler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# MSYS2 gotcha: these packages live in /mingw64/include, which is ALREADY a default
# system include dir. CMake would pass it again as -isystem, which reorders the search
# path and breaks libstdc++'s "#include_next <stdlib.h>" with a confusing
# "stdlib.h: No such file or directory". Forcing -I instead of -isystem fixes it.
set(CMAKE_NO_SYSTEM_FROM_IMPORTED ON)

# Beast is header-only, so Boost::headers is enough -- no compiled Boost libs needed.
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)          # wss:// -- Coinbase requires TLS
find_package(nlohmann_json REQUIRED)

add_executable(feed_handler src/main.cpp)

target_link_libraries(feed_handler PRIVATE
    Boost::headers
    OpenSSL::SSL
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
)

if(WIN32)
    # Winsock, and the minimum Windows version Asio needs for its API surface.
    target_link_libraries(feed_handler PRIVATE ws2_32 mswsock)
    target_compile_definitions(feed_handler PRIVATE _WIN32_WINNT=0x0601)
endif()

# Phase 1 requires TSan-clean. MinGW does not ship ThreadSanitizer, so this
# only takes effect on a clang/gcc toolchain that has it (WSL, Linux CI).
option(ENABLE_TSAN "Build with ThreadSanitizer" OFF)
if(ENABLE_TSAN)
    target_compile_options(feed_handler PRIVATE -fsanitize=thread -g -O1)
    target_link_options(feed_handler PRIVATE -fsanitize=thread)
endif()

target_compile_options(feed_handler PRIVATE -Wall -Wextra)
