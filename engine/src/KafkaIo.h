#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "Recovery.h"

struct ConsumedMessage {
    Partition   partition;
    Offset      offset;
    std::string payload;
};

class KafkaIo {
public:
    KafkaIo(const std::string& brokers, const std::string& group_id,
            const std::string& orders_topic, const std::string& deltas_topic);
    ~KafkaIo();

    KafkaIo(const KafkaIo&) = delete;
    KafkaIo& operator=(const KafkaIo&) = delete;

    std::optional<ConsumedMessage> poll(int timeout_ms);
    std::optional<std::unordered_map<Partition, Offset>> take_assignment();

    void produce(const std::string& key, const std::string& value);
    void flush();
    void commit(const ConsumedMessage& msg);
    void close();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
