package dev.nullzwo.flow;

import dev.nullzwo.Flow;
import dev.nullzwo.Message;
import dev.nullzwo.QueueLength;
import dev.nullzwo.Throughput;
import dev.nullzwo.pipe.MetricSinkPipe;
import dev.nullzwo.pipe.MetricSourcePipe;
import dev.nullzwo.pipe.SinkPipe;
import dev.nullzwo.pipe.SourcePipe;

import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.joining;

public class RoundRobinFlow implements Flow {
    private static final int MAX_ENQUEUED = 20;
    private final QueueLength queueLength;
    private final SinkPipe sink;
    private final SourcePipe[] sources;
    private boolean stopped;

    public RoundRobinFlow(SinkPipe sink, QueueLength queueLength, SourcePipe... sources) {
        this.sink = new MetricSinkPipe(sink);
        this.queueLength = queueLength;
        this.sources = Stream.of(sources).map(MetricSourcePipe::new).toArray(SourcePipe[]::new);
    }

    @Override
    public void run() {
        stopped = false;
        while (!stopped) {
            for (SourcePipe source : sources) {
                source.tryNextMessage().forEach(this::consume);
            }
        }
    }

    private void consume(Message message) {
        while (queueLength.size() > MAX_ENQUEUED) {
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
        stopped = true;
    }

    @Override
    public String describe() {
        var ids = Stream.of(sources).map(p -> p.id().id()).collect(joining(", ", "[", "]"));
        return ids + " -> " + sink.id().id();
    }
}
