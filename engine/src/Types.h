#pragma once

#include <cstdint>
#include <string>

// Prices are integer ticks, not dollars: 1 tick = $0.01.
using Price   = std::int64_t;
using Qty     = std::int64_t;
using OrderId = std::string;
using Symbol  = std::string;

enum class Side { Buy, Sell };
