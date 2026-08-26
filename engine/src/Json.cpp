#include "Json.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace {

std::optional<Side> parse_side(const json& j) {
    if (!j.is_string()) {
        return std::nullopt;
    }
    const std::string s = j.get<std::string>();
    if (s == "buy") return Side::Buy;
    if (s == "sell") return Side::Sell;
    return std::nullopt;
}

}  // namespace

std::optional<InboundMessage> parse_inbound(const std::string& line) {
    json j;
    try {
        j = json::parse(line);
    } catch (const json::parse_error&) {
        return std::nullopt;
    }

    if (!j.is_object() || !j.contains("type") || !j["type"].is_string()) {
        return std::nullopt;
    }

    const std::string type = j["type"].get<std::string>();

    try {
        if (type == "new") {
            if (!j.contains("symbol") || !j["symbol"].is_string()) return std::nullopt;
            if (!j.contains("order_id") || !j["order_id"].is_string()) return std::nullopt;
            if (!j.contains("side")) return std::nullopt;
            auto side = parse_side(j["side"]);
            if (!side.has_value()) return std::nullopt;
            if (!j.contains("price") || !j["price"].is_number_integer()) return std::nullopt;
            if (!j.contains("qty") || !j["qty"].is_number_integer()) return std::nullopt;
            if (!j.contains("ingress_ts_ns") || !j["ingress_ts_ns"].is_number_integer()) return std::nullopt;

            return NewOrder{j["symbol"].get<Symbol>(), j["order_id"].get<OrderId>(), *side,
                             j["price"].get<Price>(), j["qty"].get<Qty>(),
                             j["ingress_ts_ns"].get<std::int64_t>()};
        }
        if (type == "cancel") {
            if (!j.contains("symbol") || !j["symbol"].is_string()) return std::nullopt;
            if (!j.contains("order_id") || !j["order_id"].is_string()) return std::nullopt;
            if (!j.contains("ingress_ts_ns") || !j["ingress_ts_ns"].is_number_integer()) return std::nullopt;

            return CancelOrder{j["symbol"].get<Symbol>(), j["order_id"].get<OrderId>(),
                                j["ingress_ts_ns"].get<std::int64_t>()};
        }
        if (type == "amend") {
            if (!j.contains("symbol") || !j["symbol"].is_string()) return std::nullopt;
            if (!j.contains("order_id") || !j["order_id"].is_string()) return std::nullopt;
            if (!j.contains("price") || !j["price"].is_number_integer()) return std::nullopt;
            if (!j.contains("qty") || !j["qty"].is_number_integer()) return std::nullopt;
            if (!j.contains("ingress_ts_ns") || !j["ingress_ts_ns"].is_number_integer()) return std::nullopt;

            return AmendOrder{j["symbol"].get<Symbol>(), j["order_id"].get<OrderId>(),
                               j["price"].get<Price>(), j["qty"].get<Qty>(),
                               j["ingress_ts_ns"].get<std::int64_t>()};
        }
    } catch (const json::exception&) {
        return std::nullopt;
    }

    return std::nullopt;
}

std::string serialize(const OutboundEvent& event) {
    json j;
    if (const auto* trade = std::get_if<TradeEvent>(&event)) {
        j["type"] = "trade";
        j["symbol"] = trade->symbol;
        j["seq"] = trade->seq;
        j["maker_id"] = trade->maker_id;
        j["taker_id"] = trade->taker_id;
        j["price"] = trade->price;
        j["qty"] = trade->qty;
        j["ingress_ts_ns"] = trade->ingress_ts_ns;
    } else {
        const auto& delta = std::get<DeltaEvent>(event);
        j["type"] = "delta";
        j["symbol"] = delta.symbol;
        j["seq"] = delta.seq;
        j["side"] = (delta.side == Side::Buy) ? "bid" : "ask";
        j["price"] = delta.price;
        j["qty"] = delta.qty;
        j["ingress_ts_ns"] = delta.ingress_ts_ns;
    }
    return j.dump();
}
