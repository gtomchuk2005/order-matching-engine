#pragma once

#include <cstdint>
#include <variant>

#include "Types.h"

struct NewOrder {
    Symbol         symbol;
    OrderId        order_id;
    Side           side;
    Price          price;
    Qty            qty;
    std::int64_t   ingress_ts_ns;
};

struct CancelOrder {
    Symbol         symbol;
    OrderId        order_id;
    std::int64_t   ingress_ts_ns;
};

struct AmendOrder {
    Symbol         symbol;
    OrderId        order_id;
    Price          price;
    Qty            qty;
    std::int64_t   ingress_ts_ns;
};

using InboundMessage = std::variant<NewOrder, CancelOrder, AmendOrder>;

struct TradeEvent {
    Symbol         symbol;
    std::uint64_t  seq;
    OrderId        maker_id;
    OrderId        taker_id;
    Price          price;
    Qty            qty;
    std::int64_t   ingress_ts_ns;
};

struct DeltaEvent {
    Symbol         symbol;
    std::uint64_t  seq;
    Side           side;
    Price          price;
    Qty            qty;
    std::int64_t   ingress_ts_ns;
};

using OutboundEvent = std::variant<TradeEvent, DeltaEvent>;
