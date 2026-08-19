#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <optional>

#include "Order.h"
#include "PriceLevel.h"
#include "Types.h"

// std::greater on bids and std::less on asks put the best price at begin() on both sides
class OrderBook {
public:
    void add(Side side, Price price, const Order& order);

    std::optional<Price> best_bid() const;
    std::optional<Price> best_ask() const;

    Qty qty_at(Side side, Price price) const;
    std::size_t level_count(Side side) const;
    bool empty(Side side) const;

    const std::deque<Order>* orders_at(Side side, Price price) const;

private:
    const PriceLevel* level_at(Side side, Price price) const;

    std::map<Price, PriceLevel, std::greater<Price>> bids_;
    std::map<Price, PriceLevel, std::less<Price>>    asks_;
};
