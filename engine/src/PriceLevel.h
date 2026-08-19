#pragma once

#include <deque>

#include "Order.h"

struct PriceLevel {
    // total_qty is a cached sum of orders' quantities so top-of-book is O(1).
    Qty               total_qty = 0;
    std::deque<Order> orders;
};
