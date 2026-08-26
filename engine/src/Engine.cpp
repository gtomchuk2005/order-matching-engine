#include "Engine.h"

#include <algorithm>

namespace {

Side opposite(Side side) {
    return side == Side::Buy ? Side::Sell : Side::Buy;
}

void touch(std::vector<std::pair<Side, Price>>& touched, Side side, Price price) {
    auto match = [&](const auto& p) { return p.first == side && p.second == price; };
    if (std::find_if(touched.begin(), touched.end(), match) == touched.end()) {
        touched.emplace_back(side, price);
    }
}

Qty total_filled(const std::vector<Trade>& trades) {
    Qty sum = 0;
    for (const auto& t : trades) {
        sum += t.qty;
    }
    return sum;
}

}  // namespace

const OrderBook* Engine::book_for(const Symbol& symbol) const {
    auto found = books_.find(symbol);
    return found == books_.end() ? nullptr : &found->second;
}

std::vector<OutboundEvent> Engine::apply(const InboundMessage& msg) {
    std::vector<OutboundEvent> events;

    if (const auto* order = std::get_if<NewOrder>(&msg)) {
        OrderBook& book = books_[order->symbol];
        std::vector<Trade> trades = book.match(order->side, order->price, Order{order->order_id, order->qty});

        std::vector<std::pair<Side, Price>> touched;
        for (const auto& trade : trades) {
            touch(touched, opposite(order->side), trade.price);
        }
        if (total_filled(trades) < order->qty) {
            touch(touched, order->side, order->price);
        }

        std::uint64_t& seq = seq_[order->symbol];
        for (const auto& trade : trades) {
            events.push_back(TradeEvent{order->symbol, ++seq, trade.maker_id, trade.taker_id, trade.price,
                                         trade.qty, order->ingress_ts_ns});
        }
        for (const auto& [side, price] : touched) {
            events.push_back(
                DeltaEvent{order->symbol, ++seq, side, price, book.qty_at(side, price), order->ingress_ts_ns});
        }
        return events;
    }

    if (const auto* cancel = std::get_if<CancelOrder>(&msg)) {
        OrderBook& book = books_[cancel->symbol];
        auto loc = book.location_of(cancel->order_id);
        if (!loc.has_value()) {
            return events;
        }
        book.cancel(cancel->order_id);

        std::uint64_t& seq = seq_[cancel->symbol];
        events.push_back(DeltaEvent{cancel->symbol, ++seq, loc->first, loc->second,
                                     book.qty_at(loc->first, loc->second), cancel->ingress_ts_ns});
        return events;
    }

    const auto& amend = std::get<AmendOrder>(msg);
    OrderBook& book = books_[amend.symbol];
    auto loc = book.location_of(amend.order_id);
    if (!loc.has_value()) {
        return events;
    }
    Side original_side = loc->first;
    Price original_price = loc->second;

    auto result = book.amend(amend.order_id, amend.price, amend.qty);
    std::vector<Trade> trades = result.value_or(std::vector<Trade>{});

    std::vector<std::pair<Side, Price>> touched;
    touch(touched, original_side, original_price);
    for (const auto& trade : trades) {
        touch(touched, opposite(original_side), trade.price);
    }
    if (total_filled(trades) < amend.qty) {
        touch(touched, original_side, amend.price);
    }

    std::uint64_t& seq = seq_[amend.symbol];
    for (const auto& trade : trades) {
        events.push_back(TradeEvent{amend.symbol, ++seq, trade.maker_id, trade.taker_id, trade.price, trade.qty,
                                     amend.ingress_ts_ns});
    }
    for (const auto& [side, price] : touched) {
        events.push_back(
            DeltaEvent{amend.symbol, ++seq, side, price, book.qty_at(side, price), amend.ingress_ts_ns});
    }
    return events;
}
