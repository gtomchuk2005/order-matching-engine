#include <catch2/catch_test_macros.hpp>

#include <map>
#include <set>
#include <utility>

#include "Engine.h"
#include "Json.h"
#include "Message.h"
#include "OrderBook.h"
#include "Types.h"

namespace {

std::map<std::pair<int, Price>, Qty> snapshot(const OrderBook& book) {
    std::map<std::pair<int, Price>, Qty> result;
    for (const auto& [price, qty] : book.levels(Side::Buy)) {
        result[{0, price}] = qty;
    }
    for (const auto& [price, qty] : book.levels(Side::Sell)) {
        result[{1, price}] = qty;
    }
    return result;
}

std::set<std::pair<int, Price>> diff(const std::map<std::pair<int, Price>, Qty>& before,
                                      const std::map<std::pair<int, Price>, Qty>& after) {
    std::set<std::pair<int, Price>> changed;
    for (const auto& [key, qty] : before) {
        auto found = after.find(key);
        if (found == after.end() || found->second != qty) {
            changed.insert(key);
        }
    }
    for (const auto& [key, qty] : after) {
        auto found = before.find(key);
        if (found == before.end() || found->second != qty) {
            changed.insert(key);
        }
    }
    return changed;
}

std::set<std::pair<int, Price>> touched_levels(const std::vector<OutboundEvent>& events) {
    std::set<std::pair<int, Price>> touched;
    for (const auto& event : events) {
        if (const auto* delta = std::get_if<DeltaEvent>(&event)) {
            touched.insert({delta->side == Side::Buy ? 0 : 1, delta->price});
        }
    }
    return touched;
}

}  // namespace

TEST_CASE("round-trip serialize/parse for all five message types", "[engine]") {
    NewOrder new_order{"AAPL", "a1", Side::Buy, 10050, 10, 123};
    CancelOrder cancel{"AAPL", "a1", 123};
    AmendOrder amend{"AAPL", "a1", 10100, 5, 123};

    auto parsed_new = parse_inbound(
        R"({"type":"new","symbol":"AAPL","order_id":"a1","side":"buy","price":10050,"qty":10,"ingress_ts_ns":123})");
    REQUIRE(parsed_new.has_value());
    REQUIRE(std::get<NewOrder>(*parsed_new).symbol == new_order.symbol);
    REQUIRE(std::get<NewOrder>(*parsed_new).order_id == new_order.order_id);
    REQUIRE(std::get<NewOrder>(*parsed_new).side == new_order.side);
    REQUIRE(std::get<NewOrder>(*parsed_new).price == new_order.price);
    REQUIRE(std::get<NewOrder>(*parsed_new).qty == new_order.qty);

    auto parsed_cancel =
        parse_inbound(R"({"type":"cancel","symbol":"AAPL","order_id":"a1","ingress_ts_ns":123})");
    REQUIRE(parsed_cancel.has_value());
    REQUIRE(std::get<CancelOrder>(*parsed_cancel).order_id == cancel.order_id);

    auto parsed_amend = parse_inbound(
        R"({"type":"amend","symbol":"AAPL","order_id":"a1","price":10100,"qty":5,"ingress_ts_ns":123})");
    REQUIRE(parsed_amend.has_value());
    REQUIRE(std::get<AmendOrder>(*parsed_amend).price == amend.price);
    REQUIRE(std::get<AmendOrder>(*parsed_amend).qty == amend.qty);

    TradeEvent trade{"AAPL", 1, "m1", "t1", 10050, 4, 123};
    std::string trade_json = serialize(trade);
    REQUIRE(trade_json.find("\"type\":\"trade\"") != std::string::npos);
    REQUIRE(trade_json.find("\"maker_id\":\"m1\"") != std::string::npos);

    DeltaEvent delta{"AAPL", 2, Side::Buy, 10050, 6, 123};
    std::string delta_json = serialize(delta);
    REQUIRE(delta_json.find("\"type\":\"delta\"") != std::string::npos);
    REQUIRE(delta_json.find("\"side\":\"bid\"") != std::string::npos);
}

TEST_CASE("malformed json returns nullopt", "[engine]") {
    REQUIRE(parse_inbound("not json") == std::nullopt);
    REQUIRE(parse_inbound("{") == std::nullopt);
}

TEST_CASE("unknown type returns nullopt", "[engine]") {
    REQUIRE(parse_inbound(R"({"type":"bogus","symbol":"AAPL"})") == std::nullopt);
}

TEST_CASE("missing field returns nullopt", "[engine]") {
    REQUIRE(parse_inbound(R"({"type":"new","symbol":"AAPL","order_id":"a1","side":"buy","price":10050})") ==
            std::nullopt);
}

TEST_CASE("multi-symbol isolation", "[engine]") {
    Engine engine;
    engine.apply(NewOrder{"AAPL", "a1", Side::Buy, 10050, 10, 1});
    engine.apply(NewOrder{"MSFT", "m1", Side::Sell, 20000, 5, 2});

    const OrderBook* aapl = engine.book_for("AAPL");
    const OrderBook* msft = engine.book_for("MSFT");
    REQUIRE(aapl != nullptr);
    REQUIRE(msft != nullptr);
    REQUIRE(aapl->qty_at(Side::Buy, 10050) == 10);
    REQUIRE(aapl->qty_at(Side::Sell, 20000) == 0);
    REQUIRE(msft->qty_at(Side::Sell, 20000) == 5);
    REQUIRE(msft->qty_at(Side::Buy, 10050) == 0);
}

TEST_CASE("seq monotonic per symbol and independent across symbols", "[engine]") {
    Engine engine;
    auto e1 = engine.apply(NewOrder{"AAPL", "a1", Side::Buy, 10050, 10, 1});
    auto e2 = engine.apply(NewOrder{"MSFT", "m1", Side::Buy, 20000, 5, 2});
    auto e3 = engine.apply(NewOrder{"AAPL", "a2", Side::Buy, 10050, 5, 3});

    REQUIRE(std::get<DeltaEvent>(e1[0]).seq == 1);
    REQUIRE(std::get<DeltaEvent>(e2[0]).seq == 1);
    REQUIRE(std::get<DeltaEvent>(e3[0]).seq == 2);
}

TEST_CASE("crossing order emits trades and correct deltas", "[engine]") {
    Engine engine;
    engine.apply(NewOrder{"AAPL", "maker", Side::Sell, 10000, 20, 1});

    const OrderBook* book = engine.book_for("AAPL");
    auto before = snapshot(*book);
    auto events = engine.apply(NewOrder{"AAPL", "taker", Side::Buy, 10000, 20, 2});
    auto after = snapshot(*book);

    auto changed = diff(before, after);
    auto touched = touched_levels(events);
    REQUIRE(changed == touched);

    int trade_count = 0;
    for (const auto& event : events) {
        if (std::holds_alternative<TradeEvent>(event)) {
            ++trade_count;
        }
    }
    REQUIRE(trade_count == 1);
}

TEST_CASE("deltas carry absolute quantities", "[engine]") {
    Engine engine;
    engine.apply(NewOrder{"AAPL", "a1", Side::Buy, 10000, 10, 1});
    auto events = engine.apply(NewOrder{"AAPL", "a2", Side::Buy, 10000, 15, 2});

    REQUIRE(events.size() == 1);
    const auto& delta = std::get<DeltaEvent>(events[0]);
    REQUIRE(delta.qty == 25);
}

TEST_CASE("cancel emits a delta for the vacated level", "[engine]") {
    Engine engine;
    engine.apply(NewOrder{"AAPL", "a1", Side::Buy, 10000, 10, 1});
    engine.apply(NewOrder{"AAPL", "a2", Side::Buy, 10000, 5, 2});

    const OrderBook* book = engine.book_for("AAPL");
    auto before = snapshot(*book);
    auto events = engine.apply(CancelOrder{"AAPL", "a1", 3});
    auto after = snapshot(*book);

    REQUIRE(events.size() == 1);
    const auto& delta = std::get<DeltaEvent>(events[0]);
    REQUIRE(delta.side == Side::Buy);
    REQUIRE(delta.price == 10000);
    REQUIRE(delta.qty == 5);
    REQUIRE(diff(before, after) == touched_levels(events));
}

TEST_CASE("fully erased level emits qty 0", "[engine]") {
    Engine engine;
    engine.apply(NewOrder{"AAPL", "a1", Side::Buy, 10000, 10, 1});

    auto events = engine.apply(CancelOrder{"AAPL", "a1", 2});

    REQUIRE(events.size() == 1);
    const auto& delta = std::get<DeltaEvent>(events[0]);
    REQUIRE(delta.qty == 0);
}

TEST_CASE("cancel of unknown id emits no events", "[engine]") {
    Engine engine;
    auto events = engine.apply(CancelOrder{"AAPL", "nonexistent", 1});
    REQUIRE(events.empty());
}

TEST_CASE("amend of unknown id emits no events", "[engine]") {
    Engine engine;
    auto events = engine.apply(AmendOrder{"AAPL", "nonexistent", 10000, 10, 1});
    REQUIRE(events.empty());
}

TEST_CASE("amend rerests and emits deltas for both old and new levels", "[engine]") {
    Engine engine;
    engine.apply(NewOrder{"AAPL", "a1", Side::Buy, 10000, 10, 1});

    const OrderBook* book = engine.book_for("AAPL");
    auto before = snapshot(*book);
    auto events = engine.apply(AmendOrder{"AAPL", "a1", 10050, 15, 2});
    auto after = snapshot(*book);

    REQUIRE(diff(before, after) == touched_levels(events));
    REQUIRE(book->qty_at(Side::Buy, 10000) == 0);
    REQUIRE(book->qty_at(Side::Buy, 10050) == 15);
}

TEST_CASE("determinism: same input produces byte-identical output", "[engine]") {
    std::vector<InboundMessage> messages{
        NewOrder{"AAPL", "a1", Side::Buy, 10000, 10, 1},
        NewOrder{"AAPL", "a2", Side::Sell, 9950, 6, 2},
        AmendOrder{"AAPL", "a1", 10100, 20, 3},
        CancelOrder{"AAPL", "a2", 4},
    };

    Engine engine1;
    Engine engine2;
    std::string out1;
    std::string out2;

    for (const auto& msg : messages) {
        for (const auto& event : engine1.apply(msg)) {
            out1 += serialize(event) + "\n";
        }
    }
    for (const auto& msg : messages) {
        for (const auto& event : engine2.apply(msg)) {
            out2 += serialize(event) + "\n";
        }
    }

    REQUIRE(out1 == out2);
}
