#include <iostream>
#include <string>

#include "Engine.h"
#include "Json.h"

int main() {
    Engine engine;
    std::string line;

    while (std::getline(std::cin, line)) {
        auto msg = parse_inbound(line);
        if (!msg.has_value()) {
            std::cerr << "malformed message: " << line << "\n";
            continue;
        }
        for (const auto& event : engine.apply(*msg)) {
            std::cout << serialize(event) << "\n";
        }
    }

    return 0;
}
