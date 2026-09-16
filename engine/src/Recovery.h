#pragma once

#include <cstdint>
#include <set>
#include <unordered_map>

#include "Types.h"

using Partition = std::int32_t;
using Offset    = std::int64_t;

class Recovery {
public:
    explicit Recovery(std::unordered_map<Partition, Offset> committed);

    bool is_replay(Partition partition, Offset offset) const;
    bool record(Partition partition, Offset offset, const Symbol& symbol);
    bool record(Partition partition, Offset offset);

    const std::set<Symbol>& symbols_for(Partition partition) const;

private:
    std::unordered_map<Partition, Offset>            committed_;
    std::unordered_map<Partition, std::set<Symbol>>   symbols_;
};
