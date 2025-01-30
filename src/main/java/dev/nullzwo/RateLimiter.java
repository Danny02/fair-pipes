package dev.nullzwo;

public interface RateLimiter {
    void runWithRateLimit(Runnable runnable);

    void configure(Throughput throughput);
}
