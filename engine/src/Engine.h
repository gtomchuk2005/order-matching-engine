#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "Message.h"
#include "OrderBook.h"
#include "Types.h"

class Engine {
public:
    std::vector<OutboundEvent> apply(const InboundMessage& msg);

    const OrderBook* book_for(const Symbol& symbol) const;

private:
    std::unordered_map<Symbol, OrderBook> books_;
    std::unordered_map<Symbol, std::uint64_t> seq_;
};
