#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <random>
#include <string>
#include <vector>

#include "OrderBook.h"

namespace {

using Clock = std::chrono::steady_clock;

constexpr int kBatch = 12;
constexpr int kSamples = 4000;
constexpr int kTotalOps = kBatch * kSamples;
constexpr int kWarmupBatches = 300;
constexpr int kWarmupOps = kBatch * kWarmupBatches;

// Bids stay below 1000 and asks above 2000 so the two ladders never cross.
void seed_book(OrderBook& book, int depth, const std::vector<OrderId>& bid_ids,
                const std::vector<OrderId>& ask_ids) {
    for (int i = 0; i < depth; ++i) {
        book.add(Side::Buy, 1000 - i, Order{bid_ids[i], 100});
        book.add(Side::Sell, 2000 + i, Order{ask_ids[i], 100});
    }
}

void seed_bids_only(OrderBook& book, int depth, const std::vector<OrderId>& bid_ids) {
    for (int i = 0; i < depth; ++i) {
        book.add(Side::Buy, 1000 - i, Order{bid_ids[i], 100});
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

// samples holds one per-op average per K-sized batch: these are percentiles of batch
// means, not of individual calls, which the clock resolution cannot separate.
void report_percentiles(benchmark::State& state, std::vector<double>& samples) {
    std::sort(samples.begin(), samples.end());
    auto pct = [&](double p) {
        std::size_t idx = static_cast<std::size_t>(std::ceil(p / 100.0 * samples.size())) - 1;
        return samples[std::min(idx, samples.size() - 1)];
    };
    state.counters["p50_batch_ns"] = benchmark::Counter(pct(50.0));
    state.counters["p90_batch_ns"] = benchmark::Counter(pct(90.0));
    state.counters["p99_batch_ns"] = benchmark::Counter(pct(99.0));
    state.counters["p999_batch_ns"] = benchmark::Counter(pct(99.9));
}

void BM_InsertExistingLevel(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);

    // Whole random access sequence is generated up front so no PRNG call falls inside
    // the timed region.
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int> level_dist(0, depth - 1);
    std::vector<Price> targets(kWarmupOps + kTotalOps);
    for (auto& p : targets) p = 1000 - level_dist(rng);
    auto insert_ids = make_ids("ins", kWarmupOps + kTotalOps);

    // Rolling pool: each timed insert is paired with an untimed cancel of an order
    // inserted kBatch ops earlier so the book never drifts from its seeded depth.
    std::deque<OrderId> pool(bid_ids.begin(), bid_ids.end());

    std::vector<double> samples;
    samples.reserve(kSamples);
    int idx = 0;

    for (int s = 0; s < kWarmupBatches; ++s) {
        for (int k = 0; k < kBatch; ++k) {
            book.add(Side::Buy, targets[idx], Order{insert_ids[idx], 10});
            ++idx;
        }
        for (int k = idx - kBatch; k < idx; ++k) {
            book.cancel(pool.front());
            pool.pop_front();
            pool.push_back(insert_ids[k]);
        }
    }

    for (auto _ : state) {
        for (int s = 0; s < kSamples; ++s) {
            auto t0 = Clock::now();
            for (int k = 0; k < kBatch; ++k) {
                book.add(Side::Buy, targets[idx], Order{insert_ids[idx], 10});
                benchmark::DoNotOptimize(insert_ids[idx]);
                ++idx;
            }
            auto t1 = Clock::now();
            benchmark::ClobberMemory();
            samples.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / kBatch);

            for (int k = idx - kBatch; k < idx; ++k) {
                book.cancel(pool.front());
                pool.pop_front();
                pool.push_back(insert_ids[k]);
            }
        }
    }

    report_percentiles(state, samples);
}
BENCHMARK(BM_InsertExistingLevel)->Arg(10)->Arg(100)->Arg(1000)->Iterations(1)->Unit(benchmark::kNanosecond);

void BM_InsertNewLevel(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);

    // Prices sit below the seeded bid ladder, so every insert creates a genuinely unseen level.
    std::vector<Price> targets(kWarmupOps + kTotalOps);
    for (int i = 0; i < kWarmupOps + kTotalOps; ++i) targets[i] = -1000 - depth - i;
    std::mt19937 rng(0xFACADE);
    std::shuffle(targets.begin(), targets.end(), rng);
    auto insert_ids = make_ids("new", kWarmupOps + kTotalOps);

    std::vector<double> samples;
    samples.reserve(kSamples);
    int idx = 0;

    for (int s = 0; s < kWarmupBatches; ++s) {
        for (int k = 0; k < kBatch; ++k) {
            book.add(Side::Buy, targets[idx], Order{insert_ids[idx], 10});
            ++idx;
        }
        for (int k = idx - kBatch; k < idx; ++k) {
            book.cancel(insert_ids[k]);
        }
    }

    for (auto _ : state) {
        for (int s = 0; s < kSamples; ++s) {
            auto t0 = Clock::now();
            for (int k = 0; k < kBatch; ++k) {
                book.add(Side::Buy, targets[idx], Order{insert_ids[idx], 10});
                benchmark::DoNotOptimize(insert_ids[idx]);
                ++idx;
            }
            auto t1 = Clock::now();
            benchmark::ClobberMemory();
            samples.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / kBatch);

            // Untimed: remove the levels just created so the book never grows past depth.
            for (int k = idx - kBatch; k < idx; ++k) {
                book.cancel(insert_ids[k]);
            }
        }
    }

    report_percentiles(state, samples);
}
BENCHMARK(BM_InsertNewLevel)->Arg(10)->Arg(100)->Arg(1000)->Iterations(1)->Unit(benchmark::kNanosecond);

void BM_Cancel(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    auto bid_ids = make_ids("bid", depth);
    auto ask_ids = make_ids("ask", depth);
    OrderBook book;
    seed_book(book, depth, bid_ids, ask_ids);

    // Each level gets its own pool of kBatch spare orders, so even a batch that draws
    // the same level kBatch times in a row never runs out of ids to cancel.
    std::vector<std::deque<OrderId>> extra_pool(depth);
    int uid = 0;
    for (int lvl = 0; lvl < depth; ++lvl) {
        for (int k = 0; k < kBatch; ++k) {
            OrderId id = "extra" + std::to_string(lvl) + "_" + std::to_string(uid++);
            book.add(Side::Buy, 1000 - lvl, Order{id, 10});
            extra_pool[lvl].push_back(id);
        }
    }

    std::mt19937 rng(0xBEEF);
    std::uniform_int_distribution<int> level_dist(0, depth - 1);
    std::vector<int> targets(kWarmupOps + kTotalOps);
    for (auto& lvl : targets) lvl = level_dist(rng);

    std::vector<double> samples;
    samples.reserve(kSamples);
    std::vector<int> batch_levels(kBatch);
    int idx = 0;

    for (int s = 0; s < kWarmupBatches; ++s) {
        for (int k = 0; k < kBatch; ++k) {
            int lvl = targets[idx++];
            OrderId id = extra_pool[lvl].front();
            extra_pool[lvl].pop_front();
            book.cancel(id);
            batch_levels[k] = lvl;
        }
        for (int k = 0; k < kBatch; ++k) {
            int lvl = batch_levels[k];
            OrderId id = "extra" + std::to_string(lvl) + "_" + std::to_string(uid++);
            book.add(Side::Buy, 1000 - lvl, Order{id, 10});
            extra_pool[lvl].push_back(id);
        }
    }

    for (auto _ : state) {
        for (int s = 0; s < kSamples; ++s) {
            auto t0 = Clock::now();
            for (int k = 0; k < kBatch; ++k) {
                int lvl = targets[idx++];
                OrderId id = extra_pool[lvl].front();
                extra_pool[lvl].pop_front();
                bool ok = book.cancel(id);
                benchmark::DoNotOptimize(ok);
                batch_levels[k] = lvl;
            }
            auto t1 = Clock::now();
            benchmark::ClobberMemory();
            samples.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / kBatch);

            for (int k = 0; k < kBatch; ++k) {
                int lvl = batch_levels[k];
                OrderId id = "extra" + std::to_string(lvl) + "_" + std::to_string(uid++);
                book.add(Side::Buy, 1000 - lvl, Order{id, 10});
                extra_pool[lvl].push_back(id);
            }
        }
    }

    report_percentiles(state, samples);
}
BENCHMARK(BM_Cancel)->Arg(10)->Arg(100)->Arg(1000)->Iterations(1)->Unit(benchmark::kNanosecond);

// Far above any resting ask price, so a taker at this limit always crosses the best ask.
constexpr Price kFarLimit = 1'000'000'000;

void BM_MatchSingleLevel(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    const int total_ops = kWarmupOps + kTotalOps;
    auto bid_ids = make_ids("bid", depth);
    OrderBook book;
    seed_bids_only(book, depth, bid_ids);

    // Rolling ask ladder: each match consumes the lowest level, then an untimed add
    // restores it at a fresh, higher price so depth never drifts.
    auto target_ids = make_ids("targetA", depth + total_ops);
    Price next_price = 2000;
    int next_id = 0;
    for (int i = 0; i < depth; ++i) {
        book.add(Side::Sell, next_price++, Order{target_ids[next_id++], 100});
    }

    auto taker_ids = make_ids("takerA", total_ops);

    std::vector<double> samples;
    samples.reserve(kSamples);
    int idx = 0;

    for (int s = 0; s < kWarmupBatches; ++s) {
        for (int k = 0; k < kBatch; ++k) {
            book.match(Side::Buy, kFarLimit, Order{taker_ids[idx], 100});
            book.add(Side::Sell, next_price++, Order{target_ids[next_id++], 100});
            ++idx;
        }
    }

    for (auto _ : state) {
        for (int s = 0; s < kSamples; ++s) {
            auto t0 = Clock::now();
            for (int k = 0; k < kBatch; ++k) {
                auto trades = book.match(Side::Buy, kFarLimit, Order{taker_ids[idx], 100});
                benchmark::DoNotOptimize(trades);
                ++idx;
            }
            auto t1 = Clock::now();
            benchmark::ClobberMemory();
            samples.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / kBatch);

            for (int k = 0; k < kBatch; ++k) {
                book.add(Side::Sell, next_price++, Order{target_ids[next_id++], 100});
            }
        }
    }

    report_percentiles(state, samples);
}
BENCHMARK(BM_MatchSingleLevel)->Arg(10)->Arg(100)->Arg(1000)->Iterations(1)->Unit(benchmark::kNanosecond);

void BM_MatchSweep(benchmark::State& state) {
    const int depth = static_cast<int>(state.range(0));
    const int sweep_levels = std::min(depth, 5);
    const int total_ops = kWarmupOps + kTotalOps;
    auto bid_ids = make_ids("bid", depth);
    OrderBook book;
    seed_bids_only(book, depth, bid_ids);

    // Rolling ask ladder: each match sweeps the sweep_levels lowest levels, then an
    // untimed add restores that many fresh, higher-priced levels.
    auto target_ids = make_ids("targetB", static_cast<std::size_t>(depth + total_ops) * sweep_levels);
    Price next_price = 4000;
    int next_id = 0;
    for (int i = 0; i < depth; ++i) {
        book.add(Side::Sell, next_price++, Order{target_ids[next_id++], 100});
    }

    auto taker_ids = make_ids("takerB", total_ops);

    std::vector<double> samples;
    samples.reserve(kSamples);
    int idx = 0;

    for (int s = 0; s < kWarmupBatches; ++s) {
        for (int k = 0; k < kBatch; ++k) {
            book.match(Side::Buy, kFarLimit, Order{taker_ids[idx], 100 * sweep_levels});
            for (int lvl = 0; lvl < sweep_levels; ++lvl) {
                book.add(Side::Sell, next_price++, Order{target_ids[next_id++], 100});
            }
            ++idx;
        }
    }

    for (auto _ : state) {
        for (int s = 0; s < kSamples; ++s) {
            auto t0 = Clock::now();
            for (int k = 0; k < kBatch; ++k) {
                auto trades = book.match(Side::Buy, kFarLimit,
                                          Order{taker_ids[idx], 100 * sweep_levels});
                benchmark::DoNotOptimize(trades);
                ++idx;
            }
            auto t1 = Clock::now();
            benchmark::ClobberMemory();
            samples.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / kBatch);

            for (int k = 0; k < kBatch; ++k) {
                for (int lvl = 0; lvl < sweep_levels; ++lvl) {
                    book.add(Side::Sell, next_price++, Order{target_ids[next_id++], 100});
                }
            }
        }
    }

    report_percentiles(state, samples);
}
BENCHMARK(BM_MatchSweep)->Arg(10)->Arg(100)->Arg(1000)->Iterations(1)->Unit(benchmark::kNanosecond);

}  // namespace

BENCHMARK_MAIN();
