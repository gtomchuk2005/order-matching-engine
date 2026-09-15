#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "Message.h"

class OrderBook;

std::optional<InboundMessage> parse_inbound(const std::string& line);
std::string serialize(const OutboundEvent& event);
std::string snapshot_json(const Symbol& symbol, std::uint64_t seq, const OrderBook& book);
