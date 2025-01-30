package dev.nullzwo;

import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;

import java.time.Duration;

import static io.github.resilience4j.ratelimiter.RateLimiterConfig.custom;
import static java.time.Duration.ofSeconds;

public class Resilience4JRateLimiter implements RateLimiter {

    private final io.github.resilience4j.ratelimiter.RateLimiter rateLimiterDelegate;

    public Resilience4JRateLimiter() {
        RateLimiterRegistry rateLimiterRegistry = RateLimiterRegistry.of(
                custom()
                        .limitRefreshPeriod(ofSeconds(1))
                        .limitForPeriod(Integer.MAX_VALUE)
                        .timeoutDuration(ofSeconds(10))
                        .build()
        );
        this.rateLimiterDelegate = rateLimiterRegistry.rateLimiter("rateLimiter");
    }

    /**
     * Executes the runnable after potentially blocking for some time
     * (depending on the rate limit and how many runnables were executed shortly before).
     * If the wait time would be bigger than the configured <code>timeoutDuration</code>,
     * an exception is thrown instead.
     */
    @Override
    public void runWithRateLimit(Runnable runnable) {
        try {
            rateLimiterDelegate.executeRunnable(runnable);
        } catch (RequestNotPermitted exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void configure(Throughput throughput) {
        var countPerSecond = (int)(throughput.messageCount() * (throughput.duration().toMillis() / 1000.0));
        rateLimiterDelegate.changeLimitForPeriod(countPerSecond);
    }
}
