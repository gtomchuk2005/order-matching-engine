#include "OrderBook.h"

void OrderBook::add(Side side, Price price, const Order& order) {
    PriceLevel& level = (side == Side::Buy) ? bids_[price] : asks_[price];
    level.orders.push_back(order);
    level.total_qty += order.qty;
}

std::optional<Price> OrderBook::best_bid() const {
    if (bids_.empty()) {
        return std::nullopt;
    }
    return bids_.begin()->first;
}

std::optional<Price> OrderBook::best_ask() const {
    if (asks_.empty()) {
        return std::nullopt;
    }
    return asks_.begin()->first;
}

const PriceLevel* OrderBook::level_at(Side side, Price price) const {
    if (side == Side::Buy) {
        auto it = bids_.find(price);
        return (it == bids_.end()) ? nullptr : &it->second;
    }
    auto it = asks_.find(price);
    return (it == asks_.end()) ? nullptr : &it->second;
}

Qty OrderBook::qty_at(Side side, Price price) const {
    const PriceLevel* level = level_at(side, price);
    return level ? level->total_qty : 0;
}

std::size_t OrderBook::level_count(Side side) const {
    return (side == Side::Buy) ? bids_.size() : asks_.size();
}

bool OrderBook::empty(Side side) const {
    return (side == Side::Buy) ? bids_.empty() : asks_.empty();
}

const std::deque<Order>* OrderBook::orders_at(Side side, Price price) const {
    const PriceLevel* level = level_at(side, price);
    return level ? &level->orders : nullptr;
}
