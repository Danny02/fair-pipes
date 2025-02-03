package dev.nullzwo.flow;

import dev.nullzwo.Throughput;

public interface RateLimiter {
    void runWithRateLimit(Runnable runnable);

    void configure(Throughput throughput);
}
