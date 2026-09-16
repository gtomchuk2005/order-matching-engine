#include "KafkaIo.h"

#include <rdkafkacpp.h>

#include <stdexcept>

namespace {

std::string conf_set(RdKafka::Conf& conf, const std::string& name, const std::string& value) {
    std::string errstr;
    if (conf.set(name, value, errstr) != RdKafka::Conf::CONF_OK) {
        return errstr;
    }
    return "";
}

}  // namespace

struct KafkaIo::Impl : public RdKafka::RebalanceCb, public RdKafka::DeliveryReportCb {
    std::string orders_topic;
    std::string deltas_topic;

    std::unique_ptr<RdKafka::KafkaConsumer> consumer;
    std::unique_ptr<RdKafka::Producer>      producer;

    std::optional<std::unordered_map<Partition, Offset>> pending_assignment;
    bool                                                  closing = false;
    bool                                                  fatal   = false;
    std::string                                           fatal_msg;

    int  delivery_errors = 0;
    std::string first_delivery_error;

    // Must not throw: librdkafka calls this from C; poll() rethrows via fatal.
    void rebalance_cb(RdKafka::KafkaConsumer* c, RdKafka::ErrorCode err,
                       std::vector<RdKafka::TopicPartition*>& partitions) override {
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            RdKafka::ErrorCode committed_err = c->committed(partitions, 10000);
            if (committed_err != RdKafka::ERR_NO_ERROR) {
                c->unassign();
                fatal     = true;
                fatal_msg = RdKafka::err2str(committed_err);
                return;
            }
            std::unordered_map<Partition, Offset> assignment;
            for (auto* tp : partitions) {
                assignment[tp->partition()] = tp->offset();
                tp->set_offset(RdKafka::Topic::OFFSET_BEGINNING);
            }
            c->assign(partitions);
            pending_assignment = std::move(assignment);
        } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
            c->unassign();
            if (!closing) {
                fatal     = true;
                fatal_msg = "partitions revoked unexpectedly";
            }
        } else {
            c->unassign();
            fatal     = true;
            fatal_msg = RdKafka::err2str(err);
        }
    }

    void dr_cb(RdKafka::Message& message) override {
        if (message.err() != RdKafka::ERR_NO_ERROR) {
            ++delivery_errors;
            if (first_delivery_error.empty()) {
                first_delivery_error = message.errstr();
            }
        }
    }
};

KafkaIo::KafkaIo(const std::string& brokers, const std::string& group_id,
                  const std::string& orders_topic, const std::string& deltas_topic)
    : impl_(std::make_unique<Impl>()) {
    impl_->orders_topic = orders_topic;
    impl_->deltas_topic = deltas_topic;

    std::string errstr;

    std::unique_ptr<RdKafka::Conf> cconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (auto& [name, value] :
         {std::pair<std::string, std::string>{"bootstrap.servers", brokers},
          {"group.id", group_id},
          {"enable.auto.commit", "false"},
          {"enable.auto.offset.store", "false"},
          {"auto.offset.reset", "earliest"}}) {
        errstr = conf_set(*cconf, name, value);
        if (!errstr.empty()) throw std::runtime_error("kafka consumer config: " + errstr);
    }
    if (cconf->set("rebalance_cb", static_cast<RdKafka::RebalanceCb*>(impl_.get()), errstr) !=
        RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("kafka consumer config: " + errstr);
    }

    impl_->consumer.reset(RdKafka::KafkaConsumer::create(cconf.get(), errstr));
    if (!impl_->consumer) throw std::runtime_error("kafka consumer create failed: " + errstr);

    RdKafka::ErrorCode sub_err = impl_->consumer->subscribe({orders_topic});
    if (sub_err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("kafka subscribe failed: " + RdKafka::err2str(sub_err));
    }

    std::unique_ptr<RdKafka::Conf> pconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (auto& [name, value] :
         {std::pair<std::string, std::string>{"bootstrap.servers", brokers},
          {"enable.idempotence", "true"},
          {"acks", "all"}}) {
        errstr = conf_set(*pconf, name, value);
        if (!errstr.empty()) throw std::runtime_error("kafka producer config: " + errstr);
    }
    if (pconf->set("dr_cb", static_cast<RdKafka::DeliveryReportCb*>(impl_.get()), errstr) !=
        RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("kafka producer config: " + errstr);
    }

    impl_->producer.reset(RdKafka::Producer::create(pconf.get(), errstr));
    if (!impl_->producer) throw std::runtime_error("kafka producer create failed: " + errstr);
}

KafkaIo::~KafkaIo() = default;

std::optional<ConsumedMessage> KafkaIo::poll(int timeout_ms) {
    if (impl_->fatal) {
        throw std::runtime_error("kafka rebalance error: " + impl_->fatal_msg);
    }

    std::unique_ptr<RdKafka::Message> msg(impl_->consumer->consume(timeout_ms));

    if (impl_->fatal) {
        throw std::runtime_error("kafka rebalance error: " + impl_->fatal_msg);
    }

    switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            return ConsumedMessage{msg->partition(), msg->offset(),
                                    std::string(static_cast<const char*>(msg->payload()), msg->len())};
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            return std::nullopt;
        default:
            throw std::runtime_error("kafka consume error: " + msg->errstr());
    }
}

std::optional<std::unordered_map<Partition, Offset>> KafkaIo::take_assignment() {
    auto assignment = std::move(impl_->pending_assignment);
    impl_->pending_assignment.reset();
    return assignment;
}

void KafkaIo::produce(const std::string& key, const std::string& value) {
    RdKafka::ErrorCode err;
    do {
        err = impl_->producer->produce(
            impl_->deltas_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(value.data()), value.size(), key.data(), key.size(), 0, nullptr);
        if (err == RdKafka::ERR__QUEUE_FULL) {
            impl_->producer->poll(1000);
        }
    } while (err == RdKafka::ERR__QUEUE_FULL);

    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("kafka produce failed: " + RdKafka::err2str(err));
    }
}

void KafkaIo::flush() {
    RdKafka::ErrorCode err = impl_->producer->flush(30000);
    if (err != RdKafka::ERR_NO_ERROR || impl_->producer->outq_len() > 0) {
        throw std::runtime_error("kafka flush timed out");
    }
    if (impl_->delivery_errors > 0) {
        std::string msg = impl_->first_delivery_error;
        impl_->delivery_errors      = 0;
        impl_->first_delivery_error = "";
        throw std::runtime_error("kafka delivery failed: " + msg);
    }
    impl_->delivery_errors      = 0;
    impl_->first_delivery_error = "";
}

void KafkaIo::commit(const ConsumedMessage& msg) {
    std::unique_ptr<RdKafka::TopicPartition> tp(
        RdKafka::TopicPartition::create(impl_->orders_topic, msg.partition, msg.offset + 1));
    std::vector<RdKafka::TopicPartition*> partitions{tp.get()};
    RdKafka::ErrorCode err = impl_->consumer->commitSync(partitions);
    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("kafka commit failed: " + RdKafka::err2str(err));
    }
}

void KafkaIo::close() {
    if (!impl_->consumer) return;
    impl_->closing = true;
    impl_->consumer->close();
}
