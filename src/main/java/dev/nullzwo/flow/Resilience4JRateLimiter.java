package dev.nullzwo.flow;

import dev.nullzwo.Throughput;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;

import java.time.Duration;

import static io.github.resilience4j.ratelimiter.RateLimiterConfig.custom;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

public class Resilience4JRateLimiter implements RateLimiter {

    public static final Duration REFRESH_PERIOD = ofMillis(200);
    private final io.github.resilience4j.ratelimiter.RateLimiter inner;

    public Resilience4JRateLimiter() {
        var rateLimiterRegistry = RateLimiterRegistry.of(
                custom()
                        .limitRefreshPeriod(REFRESH_PERIOD)
                        .limitForPeriod(Integer.MAX_VALUE)
                        .timeoutDuration(ofSeconds(10))
                        .build()
        );
        inner = rateLimiterRegistry.rateLimiter("rateLimiter");
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
            inner.executeRunnable(runnable);
        } catch (RequestNotPermitted exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void configure(Throughput throughput) {
        var countPerSecond = (int) (throughput.messageCount() *
                                    (throughput.duration().toMillis() / (double) REFRESH_PERIOD.toMillis()));
        inner.changeLimitForPeriod(countPerSecond);
    }
}
