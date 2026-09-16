#include "Recovery.h"

#include <utility>

Recovery::Recovery(std::unordered_map<Partition, Offset> committed) : committed_(std::move(committed)) {}

bool Recovery::is_replay(Partition partition, Offset offset) const {
    auto found = committed_.find(partition);
    if (found == committed_.end() || found->second < 0) {
        return false;
    }
    return offset < found->second;
}

bool Recovery::record(Partition partition, Offset offset, const Symbol& symbol) {
    symbols_[partition].insert(symbol);
    auto found = committed_.find(partition);
    return found != committed_.end() && offset == found->second - 1;
}

bool Recovery::record(Partition partition, Offset offset) {
    auto found = committed_.find(partition);
    return found != committed_.end() && offset == found->second - 1;
}

const std::set<Symbol>& Recovery::symbols_for(Partition partition) const {
    static const std::set<Symbol> empty;
    auto found = symbols_.find(partition);
    return found == symbols_.end() ? empty : found->second;
}
