package dev.nullzwo;

import java.time.Duration;

import static java.time.Duration.ofSeconds;

public class Flow implements Stopable {
    private final SourcePipe source;
    private final SinkPipe sink;
    private final RateLimiter limiter;
    private boolean stopped;

    public Flow(SourcePipe source, SinkPipe sink) {
        this.source = source;
        this.sink = sink;
        limiter = new Resilience4JRateLimiter();
    }

    public void run() {
        stopped = false;
        while (!stopped) {
            limiter.runWithRateLimit(() -> sink.consume(source.nextMessage()));
        }
    }

    public void configure(Throughput throughput) {
        limiter.configure(throughput);
    }

    @Override
    public void stop() {
        System.out.println("Stopping flow: " + source.id().id() + " -> " + sink.id().id());
        stopped = true;
    }
}
