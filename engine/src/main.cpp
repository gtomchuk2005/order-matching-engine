#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "Engine.h"
#include "Json.h"
#include "KafkaIo.h"
#include "Recovery.h"
#include "RedisSink.h"

namespace {

Symbol symbol_of(const InboundMessage& msg) {
    return std::visit([](const auto& m) { return m.symbol; }, msg);
}

void write_snapshot(RedisSink* sink, Engine& engine, const Symbol& symbol) {
    if (!sink) return;
    const OrderBook* book = engine.book_for(symbol);
    if (!book) return;
    sink->set("book:" + symbol, snapshot_json(symbol, engine.seq_for(symbol), *book));
}

std::string env_or(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return (value != nullptr && value[0] != '\0') ? value : fallback;
}

volatile std::sig_atomic_t g_stop = 0;

void on_signal(int) { g_stop = 1; }

int run_stdin(Engine& engine, RedisSink* sink) {
    std::string line;
    while (std::getline(std::cin, line)) {
        auto msg = parse_inbound(line);
        if (!msg.has_value()) {
            std::cerr << "malformed message: " << line << "\n";
            continue;
        }
        auto events = engine.apply(*msg);
        for (const auto& event : events) {
            std::cout << serialize(event) << "\n";
        }

        if (!events.empty()) {
            auto symbol =
                std::visit([](const auto& e) { return e.symbol; }, events.back());
            try {
                write_snapshot(sink, engine, symbol);
            } catch (const std::exception& e) {
                std::cerr << e.what() << "\n";
                return 1;
            }
        }
    }
    return 0;
}

int run_kafka(Engine& engine, RedisSink* sink) {
    std::string brokers    = env_or("KAFKA_BROKERS", "");
    std::string orders     = env_or("ORDERS_TOPIC", "orders");
    std::string deltas     = env_or("DELTAS_TOPIC", "deltas");
    std::string group_id   = env_or("KAFKA_GROUP_ID", "matching-engine");

    KafkaIo kafka(brokers, group_id, orders, deltas);
    Recovery recovery({});

    while (!g_stop) {
        auto consumed = kafka.poll(200);

        if (auto assignment = kafka.take_assignment()) {
            recovery = Recovery(*assignment);
            for (const auto& [partition, offset] : *assignment) {
                std::cerr << "partition " << partition << " committed offset " << offset << "\n";
            }
        }

        if (!consumed.has_value()) continue;

        if (recovery.is_replay(consumed->partition, consumed->offset)) {
            auto msg = parse_inbound(consumed->payload);
            bool last;
            if (msg.has_value()) {
                engine.apply(*msg);
                last = recovery.record(consumed->partition, consumed->offset, symbol_of(*msg));
            } else {
                std::cerr << "malformed message: " << consumed->payload << "\n";
                last = recovery.record(consumed->partition, consumed->offset);
            }
            if (last) {
                for (const auto& symbol : recovery.symbols_for(consumed->partition)) {
                    write_snapshot(sink, engine, symbol);
                }
                std::cerr << "partition " << consumed->partition << " live at offset "
                          << consumed->offset << "\n";
            }
            continue;
        }

        auto msg = parse_inbound(consumed->payload);
        if (!msg.has_value()) {
            std::cerr << "malformed message: " << consumed->payload << "\n";
            kafka.commit(*consumed);
            continue;
        }

        auto events = engine.apply(*msg);
        for (const auto& event : events) {
            std::visit([&](const auto& e) { kafka.produce(e.symbol, serialize(event)); }, event);
        }
        kafka.flush();

        if (!events.empty()) {
            auto symbol = std::visit([](const auto& e) { return e.symbol; }, events.back());
            write_snapshot(sink, engine, symbol);
        }

        kafka.commit(*consumed);
    }

    kafka.close();
    return 0;
}

}  // namespace

int main() {
    std::unique_ptr<RedisSink> sink;
    const char* redis_host = std::getenv("REDIS_HOST");
    if (redis_host != nullptr && redis_host[0] != '\0') {
        const char* redis_port = std::getenv("REDIS_PORT");
        int port = redis_port ? std::atoi(redis_port) : 6379;
        try {
            sink = std::make_unique<RedisSink>(redis_host, port);
        } catch (const std::exception& e) {
            std::cerr << e.what() << "\n";
            return 1;
        }
    }

    Engine engine;

    const char* kafka_brokers = std::getenv("KAFKA_BROKERS");
    if (kafka_brokers == nullptr || kafka_brokers[0] == '\0') {
        return run_stdin(engine, sink.get());
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    try {
        return run_kafka(engine, sink.get());
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
}
