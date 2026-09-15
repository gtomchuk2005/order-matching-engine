#include "RedisSink.h"

// Not <hiredis/hiredis.h>: FetchContent puts the header at the include root.
#include <hiredis.h>

#include <stdexcept>

RedisSink::RedisSink(const std::string& host, int port) {
    ctx_ = redisConnect(host.c_str(), port);
    if (ctx_ == nullptr || ctx_->err) {
        std::string msg = ctx_ ? ctx_->errstr : "redisConnect returned null context";
        if (ctx_) redisFree(ctx_);
        throw std::runtime_error("redis connect failed: " + msg);
    }
}

RedisSink::~RedisSink() {
    if (ctx_) redisFree(ctx_);
}

void RedisSink::set(const std::string& key, const std::string& value) {
    auto* reply = static_cast<redisReply*>(
        redisCommand(ctx_, "SET %b %b", key.data(), key.size(), value.data(), value.size()));
    if (reply == nullptr) {
        throw std::runtime_error("redis SET failed: " + std::string(ctx_->errstr));
    }
    const bool is_error = reply->type == REDIS_REPLY_ERROR;
    std::string err = is_error ? reply->str : "";
    freeReplyObject(reply);
    if (is_error) {
        throw std::runtime_error("redis SET failed: " + err);
    }
}
