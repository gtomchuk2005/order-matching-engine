#include "OrderBook.h"

#include <algorithm>

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

PriceLevel* OrderBook::crossing_level(Side side, Price limit, Price& out_price) {
    if (side == Side::Buy) {
        if (asks_.empty()) return nullptr;
        auto it = asks_.begin();
        if (it->first > limit) return nullptr;
        out_price = it->first;
        return &it->second;
    }
    if (bids_.empty()) return nullptr;
    auto it = bids_.begin();
    if (it->first < limit) return nullptr;
    out_price = it->first;
    return &it->second;
}

void OrderBook::erase_level(Side side, Price price) {
    if (side == Side::Buy) {
        bids_.erase(price);
    } else {
        asks_.erase(price);
    }
}

std::vector<Trade> OrderBook::match(Side side, Price price, const Order& order) {
    std::vector<Trade> trades;
    Qty remaining = order.qty;

    while (remaining > 0) {
        Price best_price = 0;
        PriceLevel* level = crossing_level(side, price, best_price);
        if (level == nullptr) break;

        Order& maker = level->orders.front();
        Qty fill = std::min(remaining, maker.qty);

        // Trade executes at the resting (maker) order's price, not the incoming price.
        trades.push_back(Trade{maker.id, order.id, best_price, fill});

        remaining -= fill;
        maker.qty -= fill;
        level->total_qty -= fill;

        if (maker.qty == 0) {
            level->orders.pop_front();
        }
        // level may dangle after erase, so erase last and don't touch it again.
        if (level->orders.empty()) {
            erase_level(side == Side::Buy ? Side::Sell : Side::Buy, best_price);
        }
    }

    if (remaining > 0) {
        add(side, price, Order{order.id, remaining});
    }

    return trades;
}
