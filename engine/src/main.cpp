#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "Engine.h"
#include "Json.h"
#include "RedisSink.h"

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

        if (sink && !events.empty()) {
            auto [symbol, seq] = std::visit(
                [](const auto& e) { return std::pair{e.symbol, e.seq}; }, events.back());
            try {
                sink->set("book:" + symbol, snapshot_json(symbol, seq, *engine.book_for(symbol)));
            } catch (const std::exception& e) {
                std::cerr << e.what() << "\n";
                return 1;
            }
        }
    }

    return 0;
}
