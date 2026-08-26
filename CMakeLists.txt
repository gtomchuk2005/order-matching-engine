cmake_minimum_required(VERSION 3.20)
project(order_matching_engine LANGUAGES CXX)

enable_testing()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

include(FetchContent)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG v3.7.1
)
FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

add_library(engine engine/src/OrderBook.cpp engine/src/Engine.cpp engine/src/Json.cpp)
target_include_directories(engine PUBLIC engine/src)
target_link_libraries(engine PRIVATE nlohmann_json::nlohmann_json)

add_executable(engine_main engine/src/main.cpp)
target_link_libraries(engine_main PRIVATE engine)

add_executable(engine_tests engine/test/OrderBookTest.cpp engine/test/MatchingTest.cpp engine/test/EngineTest.cpp)
target_link_libraries(engine_tests PRIVATE engine Catch2::Catch2WithMain)

list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
include(CTest)
include(Catch)
catch_discover_tests(engine_tests)
