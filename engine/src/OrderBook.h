#pragma once

#include <cstddef>
#include <functional>
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

#include "Order.h"
#include "PriceLevel.h"
#include "Trade.h"
#include "Types.h"

// std::greater on bids and std::less on asks put the best price at begin() on both sides
class OrderBook {
public:
    void add(Side side, Price price, const Order& order);

    std::vector<Trade> match(Side side, Price price, const Order& order);

    bool cancel(OrderId id);
    std::optional<std::vector<Trade>> amend(OrderId id, Price new_price, Qty new_qty);

    std::optional<Price> best_bid() const;
    std::optional<Price> best_ask() const;

    Qty qty_at(Side side, Price price) const;
    std::size_t level_count(Side side) const;
    bool empty(Side side) const;

    const std::list<Order>* orders_at(Side side, Price price) const;

private:
    struct Location {
        Side side;
        Price price;
        std::list<Order>::iterator it;
    };

    const PriceLevel* level_at(Side side, Price price) const;
    PriceLevel* crossing_level(Side side, Price limit, Price& out_price);
    void erase_level(Side side, Price price);

    std::map<Price, PriceLevel, std::greater<Price>> bids_;
    std::map<Price, PriceLevel, std::less<Price>>    asks_;
    std::unordered_map<OrderId, Location> index_;
};
