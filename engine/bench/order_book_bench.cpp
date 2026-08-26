#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

#include "OrderBook.h"

namespace {

double percentile(const std::vector<double>& values, double pct) {
    std::vector<double> sorted(values);
    std::sort(sorted.begin(), sorted.end());
    std::size_t idx = static_cast<std::size_t>(std::ceil(pct / 100.0 * sorted.size())) - 1;
    return sorted[std::min(idx, sorted.size() - 1)];
}

double p50(const std::vector<double>& v) { return percentile(v, 50.0); }
double p99(const std::vector<double>& v) { return percentile(v, 99.0); }

// Seeds N price levels per side, one order per level, away from the crossing prices
// used by match benchmarks (bids below 1000, asks above 2000).
void seed_book(OrderBook& book, int depth, const std::vector<OrderId>& bid_ids,
                const std::vector<OrderId>& ask_ids) {
    for (int i = 0; i < depth; ++i) {
        book.add(Side::Buy, 1000 - i, Order{bid_ids[i], 100});
        book.add(Side::Sell, 2000 + i, Order{ask_ids[i], 100});
    }
}

std::vector<OrderId> make_ids(const std::string& prefix, int count) {
    std::vector<OrderId> ids;
    ids.reserve(count);
    for (int i = 0; i < count; ++i) {
        ids.push_back(prefix + std::to_string(i));
    }
    return ids;
}

void BM_Insert(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    auto insert_ids = make_ids("insert", static_cast<int>(state.max_iterations));

    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);

    std::size_t i = 0;
    for (auto _ : state) {
        const OrderId& id = insert_ids[i++];
        book.add(Side::Buy, 1000, Order{id, 10});
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_Insert)->Arg(10)->Arg(100)->Arg(1000)->Iterations(5000)->Repetitions(20)->ComputeStatistics("p50", p50)->ComputeStatistics("p99", p99)->DisplayAggregatesOnly(true);

void BM_Cancel(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    auto cancel_ids = make_ids("cancel", static_cast<int>(state.max_iterations));

    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);
    for (const auto& id : cancel_ids) {
        book.add(Side::Buy, 1000, Order{id, 10});
    }

    std::size_t i = 0;
    for (auto _ : state) {
        bool ok = book.cancel(cancel_ids[i++]);
        benchmark::DoNotOptimize(ok);
    }
}
BENCHMARK(BM_Cancel)->Arg(10)->Arg(100)->Arg(1000)->Iterations(5000)->Repetitions(20)->ComputeStatistics("p50", p50)->ComputeStatistics("p99", p99)->DisplayAggregatesOnly(true);

// Pre-seeds one resting ask per iteration at a distinct price above the depth
// levels, so every timed match consumes a fresh level without re-seeding mid-loop.
void BM_MatchSingleLevel(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    const auto iters = static_cast<int>(state.max_iterations);
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    auto taker_ids = make_ids("takerA", iters);
    auto target_ids = make_ids("targetA", iters);

    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);
    for (int i = 0; i < iters; ++i) {
        book.add(Side::Sell, 3000 + i, Order{target_ids[i], 100});
    }

    std::size_t i = 0;
    for (auto _ : state) {
        auto trades = book.match(Side::Buy, 3000 + static_cast<int>(i), Order{taker_ids[i], 100});
        ++i;
        benchmark::DoNotOptimize(trades);
    }
}
BENCHMARK(BM_MatchSingleLevel)->Arg(10)->Arg(100)->Arg(1000)->Iterations(300)->Repetitions(20)->ComputeStatistics("p50", p50)->ComputeStatistics("p99", p99)->DisplayAggregatesOnly(true);

// Pre-seeds a disjoint block of sweep_levels resting asks per iteration, each block
// in its own price band above 4000, so every timed match sweeps fresh levels.
void BM_MatchSweep(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    const int sweep_levels = std::min(depth, 5);
    const auto iters = static_cast<int>(state.max_iterations);
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    auto taker_ids = make_ids("takerB", iters);
    auto target_ids = make_ids("targetB", iters * sweep_levels);

    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);
    for (int i = 0; i < iters; ++i) {
        const int base = 4000 + i * (sweep_levels + 1);
        for (int lvl = 0; lvl < sweep_levels; ++lvl) {
            book.add(Side::Sell, base + lvl, Order{target_ids[i * sweep_levels + lvl], 100});
        }
    }

    std::size_t i = 0;
    for (auto _ : state) {
        const int base = 4000 + static_cast<int>(i) * (sweep_levels + 1);
        auto trades = book.match(Side::Buy, base + sweep_levels - 1,
                                  Order{taker_ids[i], 100 * sweep_levels});
        ++i;
        benchmark::DoNotOptimize(trades);
    }
}
BENCHMARK(BM_MatchSweep)->Arg(10)->Arg(100)->Arg(1000)->Iterations(300)->Repetitions(20)->ComputeStatistics("p50", p50)->ComputeStatistics("p99", p99)->DisplayAggregatesOnly(true);

}  // namespace

BENCHMARK_MAIN();
