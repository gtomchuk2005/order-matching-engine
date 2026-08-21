#pragma once

#include "Types.h"

struct Trade {
    OrderId maker_id;
    OrderId taker_id;
    // A trade executes at the resting (maker) order's price.
    Price   price;
    Qty     qty;
};
