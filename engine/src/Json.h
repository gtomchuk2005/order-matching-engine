#pragma once

#include <optional>
#include <string>

#include "Message.h"

std::optional<InboundMessage> parse_inbound(const std::string& line);
std::string serialize(const OutboundEvent& event);
