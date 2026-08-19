#include <catch2/catch_test_macros.hpp>

#include "Order.h"
#include "OrderBook.h"
#include "Types.h"

TEST_CASE("empty book has no levels or resting orders", "[order_book]") {
    OrderBook book;

    REQUIRE(book.best_bid() == std::nullopt);
    REQUIRE(book.best_ask() == std::nullopt);
    REQUIRE(book.level_count(Side::Buy) == 0);
    REQUIRE(book.level_count(Side::Sell) == 0);
    REQUIRE(book.empty(Side::Buy));
    REQUIRE(book.empty(Side::Sell));
    REQUIRE(book.qty_at(Side::Buy, 10000) == 0);
    REQUIRE(book.qty_at(Side::Sell, 10000) == 0);
    REQUIRE(book.orders_at(Side::Buy, 10000) == nullptr);
    REQUIRE(book.orders_at(Side::Sell, 10000) == nullptr);
}

TEST_CASE("single bid becomes best bid", "[order_book]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"a", 20});

    REQUIRE(book.best_bid() == 10000);
    REQUIRE(book.best_ask() == std::nullopt);
    REQUIRE(book.qty_at(Side::Buy, 10000) == 20);
    REQUIRE(book.level_count(Side::Buy) == 1);
    REQUIRE_FALSE(book.empty(Side::Buy));
}

TEST_CASE("single ask becomes best ask", "[order_book]") {
    OrderBook book;
    book.add(Side::Sell, 10000, Order{"a", 20});

    REQUIRE(book.best_ask() == 10000);
    REQUIRE(book.best_bid() == std::nullopt);
    REQUIRE(book.qty_at(Side::Sell, 10000) == 20);
    REQUIRE(book.level_count(Side::Sell) == 1);
    REQUIRE_FALSE(book.empty(Side::Sell));
}

TEST_CASE("best bid is the highest resting price", "[order_book]") {
    OrderBook book;
    book.add(Side::Buy, 9950, Order{"a", 10});
    book.add(Side::Buy, 10000, Order{"b", 10});
    book.add(Side::Buy, 9900, Order{"c", 10});

    REQUIRE(book.best_bid() == 10000);
    REQUIRE(book.level_count(Side::Buy) == 3);
}

TEST_CASE("best ask is the lowest resting price", "[order_book]") {
    OrderBook book;
    book.add(Side::Sell, 10100, Order{"a", 10});
    book.add(Side::Sell, 10050, Order{"b", 10});
    book.add(Side::Sell, 10200, Order{"c", 10});

    REQUIRE(book.best_ask() == 10050);
}

TEST_CASE("multiple orders at one price aggregate quantity", "[order_book]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"a", 20});
    book.add(Side::Buy, 10000, Order{"b", 30});

    REQUIRE(book.qty_at(Side::Buy, 10000) == 50);
    REQUIRE(book.level_count(Side::Buy) == 1);
}

TEST_CASE("orders within a price level keep FIFO time priority", "[order_book]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"a", 10});
    book.add(Side::Buy, 10000, Order{"b", 10});
    book.add(Side::Buy, 10000, Order{"c", 10});

    const auto* orders = book.orders_at(Side::Buy, 10000);
    REQUIRE(orders != nullptr);
    REQUIRE(orders->size() == 3);
    REQUIRE((*orders)[0].id == "a");
    REQUIRE((*orders)[1].id == "b");
    REQUIRE((*orders)[2].id == "c");
}

TEST_CASE("bid and ask sides are independent", "[order_book]") {
    OrderBook book;
    book.add(Side::Buy, 10000, Order{"a", 10});
    book.add(Side::Sell, 10100, Order{"b", 10});

    REQUIRE(book.level_count(Side::Buy) == 1);
    REQUIRE(book.level_count(Side::Sell) == 1);
    REQUIRE(book.qty_at(Side::Buy, 10100) == 0);
    REQUIRE(book.qty_at(Side::Sell, 10000) == 0);
}

TEST_CASE("crossing prices are allowed on add", "[order_book]") {
    // add() only stores resting orders; matching crossed prices is a
    // separate concern handled elsewhere.
    OrderBook book;
    book.add(Side::Buy, 10100, Order{"a", 10});
    book.add(Side::Sell, 10000, Order{"b", 10});

    REQUIRE(book.best_bid() == 10100);
    REQUIRE(book.best_ask() == 10000);
}
