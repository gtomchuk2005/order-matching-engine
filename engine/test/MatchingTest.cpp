#include <catch2/catch_test_macros.hpp>

#include "Order.h"
#include "OrderBook.h"
#include "Trade.h"
#include "Types.h"

TEST_CASE("no cross leaves the incoming order resting", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10100, Order{"maker", 50});

    auto trades = book.match(Side::Buy, 10000, Order{"taker", 20});

    REQUIRE(trades.empty());
    REQUIRE(book.best_bid() == 10000);
    REQUIRE(book.qty_at(Side::Buy, 10000) == 20);
    REQUIRE(book.qty_at(Side::Sell, 10100) == 50);
}

TEST_CASE("exact fill empties both sides", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"maker", 20});

    auto trades = book.match(Side::Buy, 10000, Order{"taker", 20});

    REQUIRE(trades.size() == 1);
    REQUIRE(trades[0].maker_id == "maker");
    REQUIRE(trades[0].taker_id == "taker");
    REQUIRE(trades[0].price == 10000);
    REQUIRE(trades[0].qty == 20);
    REQUIRE(book.level_count(Side::Sell) == 0);
    REQUIRE(book.level_count(Side::Buy) == 0);
}

TEST_CASE("partial fill rests the incoming remainder", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"maker", 20});

    auto trades = book.match(Side::Buy, 10000, Order{"taker", 30});

    REQUIRE(trades.size() == 1);
    REQUIRE(trades[0].price == 10000);
    REQUIRE(trades[0].qty == 20);
    REQUIRE(book.level_count(Side::Sell) == 0);
    REQUIRE(book.qty_at(Side::Buy, 10000) == 10);
    REQUIRE(book.best_bid() == 10000);
}

TEST_CASE("partial fill leaves resting remainder in place", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"maker", 50});

    auto trades = book.match(Side::Buy, 10000, Order{"taker", 20});

    REQUIRE(trades.size() == 1);
    REQUIRE(trades[0].qty == 20);
    REQUIRE(book.qty_at(Side::Sell, 10000) == 30);
    REQUIRE(book.level_count(Side::Buy) == 0);
}

TEST_CASE("trade executes at the resting order's price", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"maker", 20});

    auto trades = book.match(Side::Buy, 10050, Order{"taker", 20});

    REQUIRE(trades.size() == 1);
    REQUIRE(trades[0].price == 10000);
    REQUIRE(trades[0].qty == 20);
}

TEST_CASE("multi-level sweep matches best price first", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"a", 10});
    book.add(Side::Sell, 10050, Order{"b", 10});
    book.add(Side::Sell, 10100, Order{"c", 10});

    auto trades = book.match(Side::Buy, 10050, Order{"taker", 25});

    REQUIRE(trades.size() == 2);
    REQUIRE(trades[0].price == 10000);
    REQUIRE(trades[0].qty == 10);
    REQUIRE(trades[1].price == 10050);
    REQUIRE(trades[1].qty == 10);
    REQUIRE(book.qty_at(Side::Buy, 10050) == 5);
    REQUIRE(book.qty_at(Side::Sell, 10100) == 10);
    REQUIRE(book.level_count(Side::Sell) == 1);
}

TEST_CASE("time priority within a level is respected", "[matching]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"first", 10});
    book.add(Side::Sell, 10000, Order{"second", 10});

    auto trades = book.match(Side::Buy, 10000, Order{"taker", 15});

    REQUIRE(trades.size() == 2);
    REQUIRE(trades[0].maker_id == "first");
    REQUIRE(trades[0].qty == 10);
    REQUIRE(trades[1].maker_id == "second");
    REQUIRE(trades[1].qty == 5);
    REQUIRE(book.qty_at(Side::Sell, 10000) == 5);

    const auto* orders = book.orders_at(Side::Sell, 10000);
    REQUIRE(orders != nullptr);
    REQUIRE(orders->size() == 1);
    REQUIRE((*orders)[0].id == "second");
}

TEST_CASE("sell side mirrors buy side matching", "[matching]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"maker", 20});

    auto trades = book.match(Side::Sell, 10000, Order{"taker", 20});

    REQUIRE(trades.size() == 1);
    REQUIRE(trades[0].price == 10000);
    REQUIRE(trades[0].qty == 20);
    REQUIRE(book.level_count(Side::Buy) == 0);
    REQUIRE(book.level_count(Side::Sell) == 0);
}

TEST_CASE("empty book rests the incoming order", "[matching]") {
    OrderBook book;

    auto trades = book.match(Side::Buy, 10000, Order{"taker", 20});

    REQUIRE(trades.empty());
    REQUIRE(book.best_bid() == 10000);
    REQUIRE(book.qty_at(Side::Buy, 10000) == 20);
}
