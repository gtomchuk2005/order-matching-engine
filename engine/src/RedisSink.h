#pragma once

#include <string>

struct redisContext;

class RedisSink {
public:
    RedisSink(const std::string& host, int port);
    ~RedisSink();

    RedisSink(const RedisSink&) = delete;
    RedisSink& operator=(const RedisSink&) = delete;

    void set(const std::string& key, const std::string& value);

private:
    redisContext* ctx_;
};
