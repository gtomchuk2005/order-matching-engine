#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <set>
#include <unordered_map>
#include <vector>

#include "Engine.h"
#include "Json.h"
#include "Message.h"
#include "OrderBook.h"
#include "Recovery.h"
#include "Types.h"

namespace {

Symbol symbol_of(const InboundMessage& msg) {
    return std::visit([](const auto& m) { return m.symbol; }, msg);
}

std::vector<InboundMessage> build_log() {
    return {
        NewOrder{"AAPL", "a1", Side::Buy, 10000, 10, 1},
        NewOrder{"MSFT", "m1", Side::Buy, 20000, 8, 2},
        NewOrder{"AAPL", "a2", Side::Sell, 10050, 5, 3},
        NewOrder{"GOOG", "g1", Side::Sell, 30000, 6, 4},
        NewOrder{"AAPL", "a3", Side::Sell, 10000, 10, 5},
        NewOrder{"MSFT", "m2", Side::Sell, 20000, 3, 6},
        NewOrder{"GOOG", "g2", Side::Buy, 30000, 6, 7},
        NewOrder{"AAPL", "a4", Side::Sell, 10060, 4, 8},
        NewOrder{"AAPL", "a5", Side::Sell, 10070, 3, 9},
        NewOrder{"AAPL", "a6", Side::Buy, 10070, 12, 10},
        NewOrder{"MSFT", "m3", Side::Sell, 20010, 6, 11},
        CancelOrder{"MSFT", "m1", 12},
        AmendOrder{"MSFT", "m3", 20010, 3, 13},
        NewOrder{"GOOG", "g3", Side::Buy, 29990, 5, 14},
        AmendOrder{"GOOG", "g3", 30000, 5, 15},
        NewOrder{"GOOG", "g4", Side::Sell, 30000, 8, 16},
        CancelOrder{"AAPL", "nonexistent1", 17},
        AmendOrder{"AAPL", "nonexistent2", 10000, 5, 18},
        NewOrder{"MSFT", "m4", Side::Buy, 19990, 4, 19},
        NewOrder{"MSFT", "m5", Side::Sell, 19990, 2, 20},
        NewOrder{"AAPL", "a7", Side::Buy, 9990, 6, 21},
        NewOrder{"AAPL", "a8", Side::Buy, 9980, 4, 22},
        NewOrder{"AAPL", "a9", Side::Sell, 9980, 10, 23},
        CancelOrder{"GOOG", "g4", 24},
        AmendOrder{"MSFT", "m4", 19990, 1, 25},
        NewOrder{"GOOG", "g5", Side::Buy, 29900, 2, 26},
        NewOrder{"GOOG", "g6", Side::Sell, 29900, 5, 27},
        AmendOrder{"GOOG", "g6", 29950, 3, 28},
        NewOrder{"AAPL", "a10", Side::Sell, 9970, 2, 29},
        NewOrder{"AAPL", "a11", Side::Buy, 9970, 2, 30},
        CancelOrder{"MSFT", "m3", 31},
    };
}

std::set<Symbol> symbols_in(const std::vector<InboundMessage>& log) {
    std::set<Symbol> symbols;
    for (const auto& msg : log) {
        symbols.insert(symbol_of(msg));
    }
    return symbols;
}

}  // namespace

TEST_CASE("replay at any split point reproduces byte-identical books and live events", "[recovery]") {
    std::vector<InboundMessage> log = build_log();
    std::set<Symbol> symbols = symbols_in(log);

    Engine engine_a;
    std::vector<std::vector<OutboundEvent>> events_a;
    events_a.reserve(log.size());
    for (const auto& msg : log) {
        events_a.push_back(engine_a.apply(msg));
    }

    for (std::size_t k = 0; k <= log.size(); ++k) {
        Engine engine_b;
        for (std::size_t i = 0; i < k; ++i) {
            engine_b.apply(log[i]);
        }
        for (std::size_t i = k; i < log.size(); ++i) {
            auto events_b = engine_b.apply(log[i]);
            REQUIRE(events_b.size() == events_a[i].size());
            for (std::size_t j = 0; j < events_b.size(); ++j) {
                REQUIRE(serialize(events_b[j]) == serialize(events_a[i][j]));
            }
        }
        for (const auto& symbol : symbols) {
            REQUIRE(engine_a.seq_for(symbol) == engine_b.seq_for(symbol));
            REQUIRE(snapshot_json(symbol, engine_a.seq_for(symbol), *engine_a.book_for(symbol)) ==
                    snapshot_json(symbol, engine_b.seq_for(symbol), *engine_b.book_for(symbol)));
        }
    }
}

TEST_CASE("recovery-driven partition replay/live split matches direct application", "[recovery]") {
    std::vector<InboundMessage> log = build_log();
    std::set<Symbol> symbols = symbols_in(log);

    Engine engine_a;
    std::vector<std::vector<OutboundEvent>> events_a;
    events_a.reserve(log.size());
    for (const auto& msg : log) {
        events_a.push_back(engine_a.apply(msg));
    }

    auto partition_of = [](const Symbol& symbol) -> Partition { return symbol == "AAPL" ? 0 : 1; };

    std::vector<Partition> partition_for_msg(log.size());
    std::vector<Offset> offset_for_msg(log.size());
    std::unordered_map<Partition, std::vector<InboundMessage>> partition_log;
    std::unordered_map<Partition, Offset> partition_count;
    for (std::size_t i = 0; i < log.size(); ++i) {
        Partition p = partition_of(symbol_of(log[i]));
        partition_for_msg[i] = p;
        offset_for_msg[i] = partition_count[p]++;
        partition_log[p].push_back(log[i]);
    }

    auto candidates = [](Offset count) {
        std::set<Offset> values{0, 1, count / 2, count};
        return values;
    };

    for (Offset c0 : candidates(partition_count[0])) {
        for (Offset c1 : candidates(partition_count[1])) {
            std::unordered_map<Partition, Offset> committed{{0, c0}, {1, c1}};
            Recovery recovery(committed);
            std::unordered_map<Partition, int> record_true_count{{0, 0}, {1, 0}};

            Engine engine_b;
            for (std::size_t i = 0; i < log.size(); ++i) {
                Partition p = partition_for_msg[i];
                Offset off = offset_for_msg[i];
                if (recovery.is_replay(p, off)) {
                    engine_b.apply(log[i]);
                    bool last = recovery.record(p, off, symbol_of(log[i]));
                    if (last) {
                        ++record_true_count[p];

                        Offset limit = std::min<Offset>(committed[p], static_cast<Offset>(partition_log[p].size()));
                        Engine reference;
                        std::set<Symbol> prefix_symbols;
                        for (Offset j = 0; j < limit; ++j) {
                            const auto& prefix_msg = partition_log[p][static_cast<std::size_t>(j)];
                            reference.apply(prefix_msg);
                            prefix_symbols.insert(symbol_of(prefix_msg));
                        }

                        REQUIRE(recovery.symbols_for(p) == prefix_symbols);
                        for (const auto& symbol : prefix_symbols) {
                            REQUIRE(snapshot_json(symbol, engine_b.seq_for(symbol), *engine_b.book_for(symbol)) ==
                                    snapshot_json(symbol, reference.seq_for(symbol), *reference.book_for(symbol)));
                        }
                    }
                } else {
                    auto events_b = engine_b.apply(log[i]);
                    REQUIRE(events_b.size() == events_a[i].size());
                    for (std::size_t j = 0; j < events_b.size(); ++j) {
                        REQUIRE(serialize(events_b[j]) == serialize(events_a[i][j]));
                    }
                }
            }

            REQUIRE(record_true_count[0] == (c0 > 0 ? 1 : 0));
            REQUIRE(record_true_count[1] == (c1 > 0 ? 1 : 0));

            for (const auto& symbol : symbols) {
                REQUIRE(engine_a.seq_for(symbol) == engine_b.seq_for(symbol));
                REQUIRE(snapshot_json(symbol, engine_a.seq_for(symbol), *engine_a.book_for(symbol)) ==
                        snapshot_json(symbol, engine_b.seq_for(symbol), *engine_b.book_for(symbol)));
            }
        }
    }
}

TEST_CASE("partition absent from map is live immediately", "[recovery]") {
    Recovery recovery({});
    REQUIRE_FALSE(recovery.is_replay(0, 0));
    REQUIRE_FALSE(recovery.is_replay(0, 100));
}

TEST_CASE("committed offset of zero is live immediately", "[recovery]") {
    Recovery recovery({{0, 0}});
    REQUIRE_FALSE(recovery.is_replay(0, 0));
}

TEST_CASE("negative committed offset means live immediately", "[recovery]") {
    Recovery recovery({{0, -1}});
    REQUIRE_FALSE(recovery.is_replay(0, 0));
}

TEST_CASE("boundary offset returns true from record exactly once at committed minus one", "[recovery]") {
    Recovery recovery({{0, 5}});
    REQUIRE(recovery.is_replay(0, 3));
    REQUIRE_FALSE(recovery.record(0, 3, "AAPL"));
    REQUIRE(recovery.is_replay(0, 4));
    REQUIRE(recovery.record(0, 4, "AAPL"));
}

TEST_CASE("record without a symbol still detects the last replay message", "[recovery]") {
    Recovery recovery({{0, 5}});
    REQUIRE_FALSE(recovery.record(0, 3));
    REQUIRE(recovery.record(0, 4));
    REQUIRE(recovery.symbols_for(0).empty());
}

TEST_CASE("offset equal to committed is live", "[recovery]") {
    Recovery recovery({{0, 5}});
    REQUIRE_FALSE(recovery.is_replay(0, 5));
}

TEST_CASE("two partitions have independent boundaries", "[recovery]") {
    Recovery recovery({{0, 3}, {1, 7}});
    REQUIRE(recovery.is_replay(0, 2));
    REQUIRE_FALSE(recovery.is_replay(0, 3));
    REQUIRE(recovery.is_replay(1, 6));
    REQUIRE_FALSE(recovery.is_replay(1, 7));
    REQUIRE(recovery.record(0, 2, "AAPL"));
    REQUIRE_FALSE(recovery.record(1, 2, "MSFT"));
}

TEST_CASE("symbols recorded per partition are tracked independently", "[recovery]") {
    Recovery recovery({{0, 10}, {1, 10}});
    recovery.record(0, 0, "AAPL");
    recovery.record(0, 1, "MSFT");
    recovery.record(1, 0, "GOOG");
    REQUIRE(recovery.symbols_for(0) == std::set<Symbol>{"AAPL", "MSFT"});
    REQUIRE(recovery.symbols_for(1) == std::set<Symbol>{"GOOG"});
    REQUIRE(recovery.symbols_for(2).empty());
}
