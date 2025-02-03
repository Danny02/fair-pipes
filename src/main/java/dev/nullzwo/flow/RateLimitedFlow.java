package dev.nullzwo.flow;

import dev.nullzwo.Flow;
import dev.nullzwo.Throughput;
import dev.nullzwo.pipe.MetricSinkPipe;
import dev.nullzwo.pipe.MetricSourcePipe;
import dev.nullzwo.pipe.PipeId;
import dev.nullzwo.pipe.SinkPipe;
import dev.nullzwo.pipe.SourcePipe;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public class RateLimitedFlow implements Flow {
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final SourcePipe[] sources;
    private final SinkPipe sink;
    private final Map<PipeId, RateLimiter> limiters = new HashMap<>();
    private boolean stopped;

    public RateLimitedFlow(SinkPipe sink, SourcePipe... sources) {
        this.sources = Stream.of(sources).map(MetricSourcePipe::new).toArray(SourcePipe[]::new);
        this.sink = new MetricSinkPipe(sink);
        for (SourcePipe source : sources) {
            var ratelimiter = new Resilience4JRateLimiter();
            limiters.put(source.id(), ratelimiter);
        }
    }

    @Override
    public void run() {
        stopped = false;

        for (var source : sources) {
            var ratelimiter = limiters.get(source.id());
            executor.execute(() -> {
                while (!stopped && !Thread.currentThread().isInterrupted()) {
                    ratelimiter.runWithRateLimit(() -> sink.consume(source.nextMessage()));
                }
            });
        }
    }

    public void configure(PipeId id, Throughput throughput) {
        limiters.get(id).configure(throughput);
    }

    @Override
    public void stop() {
        stopped = true;
        executor.shutdown();
        try {
            executor.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String describe() {
        var ids = Stream.of(sources).map(p -> p.id().id()).collect(joining(", ", "[", "]"));
        return ids + " -> " + sink.id().id();
    }
}
