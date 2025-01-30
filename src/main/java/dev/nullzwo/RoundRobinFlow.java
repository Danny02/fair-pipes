package dev.nullzwo;

import java.time.Duration;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.joining;

public class RoundRobinFlow implements Stopable {
    private static final int MAX_ENQUEUED = 20;
    private final SinkPipe sink;
    private final SourcePipe[] sources;
    private boolean stopped;
    RateLimiter rateLimiter = new Resilience4JRateLimiter();

    public RoundRobinFlow(SinkPipe sink, SourcePipe... sources) {
        this.sink = sink;
        this.sources = sources;
        rateLimiter.configure(new Throughput(10, ofSeconds(1)));
    }

    public void run() {
        stopped = false;
        while (!stopped) {
            for (SourcePipe source : sources) {
                source.tryNextMessage().ifPresent(msg -> rateLimiter.runWithRateLimit(() -> consume(msg)));
            }
        }
    }

    private void consume(Message message) {
        while(sink.enqueued() > MAX_ENQUEUED) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        sink.consume(message);
    }

    @Override
    public void stop() {
        var ids = Stream.of(sources).map(p -> p.id().id()).collect(joining(", ", "[", "]"));
        System.out.println("Stopping flow: " + ids + " -> " + sink.id().id());
        stopped = true;
    }
}
