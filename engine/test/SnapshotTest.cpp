#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>

#include "Json.h"
#include "OrderBook.h"
#include "Order.h"
#include "Types.h"

using json = nlohmann::json;

TEST_CASE("empty book snapshots with empty bids and asks", "[snapshot]") {
    OrderBook book;
    json j = json::parse(snapshot_json("AAPL", 1, book));
    REQUIRE(j["symbol"] == "AAPL");
    REQUIRE(j["seq"] == 1);
    REQUIRE(j["bids"].empty());
    REQUIRE(j["asks"].empty());
}

TEST_CASE("bids descend and asks ascend after out-of-order inserts", "[snapshot]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"b1", 5});
    book.add(Side::Buy, 10050, Order{"b2", 3});
    book.add(Side::Buy, 10020, Order{"b3", 2});
    book.add(Side::Sell, 10200, Order{"s1", 4});
    book.add(Side::Sell, 10100, Order{"s2", 1});
    book.add(Side::Sell, 10150, Order{"s3", 6});

    json j = json::parse(snapshot_json("AAPL", 2, book));

    REQUIRE(j["bids"] == json::parse(R"([[10050,3],[10020,2],[10000,5]])"));
    REQUIRE(j["asks"] == json::parse(R"([[10100,1],[10150,6],[10200,4]])"));
}

TEST_CASE("quantities aggregate at the same price level", "[snapshot]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"b1", 5});
    book.add(Side::Buy, 10000, Order{"b2", 7});

    json j = json::parse(snapshot_json("AAPL", 3, book));

    REQUIRE(j["bids"] == json::parse(R"([[10000,12]])"));
}

TEST_CASE("fully matched level disappears from snapshot", "[snapshot]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"s1", 5});
    book.match(Side::Buy, 10000, Order{"b1", 5});

    json j = json::parse(snapshot_json("AAPL", 4, book));

    REQUIRE(j["asks"].empty());
}

TEST_CASE("symbol and seq round-trip into the snapshot", "[snapshot]") {
    OrderBook book;
    json j = json::parse(snapshot_json("MSFT", 42, book));

    REQUIRE(j["symbol"] == "MSFT");
    REQUIRE(j["seq"] == 42);
}

TEST_CASE("exact string format for a small known book", "[snapshot]") {
    OrderBook book;
    book.add(Side::Buy, 10050, Order{"b1", 10});
    book.add(Side::Buy, 10040, Order{"b2", 5});
    book.add(Side::Sell, 10060, Order{"s1", 3});

    REQUIRE(snapshot_json("AAPL", 7, book) ==
            R"({"symbol":"AAPL","seq":7,"bids":[[10050,10],[10040,5]],"asks":[[10060,3]]})");
}
