package dev.nullzwo;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Simulation {
    private final ThroughputReconciliation throughputReconciliation;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
    private final List<ScheduledFuture<?>> jobs = new ArrayList<>();
    private final List<Stopable> flows = new ArrayList<>();

    public Simulation() {
        this.throughputReconciliation = new ThroughputReconciliation();
    }

    public void run() {
        var id1 = new PipeId("input_1");
        var input1 = new SimulatedPipe(id1);
        var id2 = new PipeId("input_2");
        var input2 = new SimulatedPipe(id2);

        var output = new SimulatedPipe(new PipeId("output"));


        schedule(new Throughput(1, ofSeconds(5)), () -> input1.consume(new SimMessage(id1, Instant.now())));
        schedule(new Throughput(20, ofSeconds(1)), () -> input2.consume(new SimMessage(id2, Instant.now())));
        schedule(new Throughput(10, ofSeconds(1)), output::nextMessage);
        System.out.println("scheduled jobs");

        var flow = new RoundRobinFlow(output, input1, input2);
        flows.add(flow);

        scheduler.execute(flow::run);
        System.out.println("started flows");
    }

    public void stop() {
        for (var flow : flows) {
            flow.stop();
        }
        System.out.println("stopped flows");
        for (var job : jobs) {
            job.cancel(true);
        }
        System.out.println("stopped jobs");
        scheduler.shutdownNow();
    }

    private void schedule(Throughput throughput, Runnable runnable) {
        var nsPerOp = throughput.duration().toNanos() / throughput.messageCount();
        jobs.add(scheduler.scheduleAtFixedRate(runnable, 0, nsPerOp, NANOSECONDS));
    }

    public static class SimulatedPipe implements SourcePipe, SinkPipe {
        private static final LongGauge MESSAGE_COUNT = App.METER.gaugeBuilder("messages_queued")
                                                                .ofLongs().build();
        private static final LongCounter READ_MESSAGES = App.METER.counterBuilder("messages_read_total").build();
        private static final LongCounter READ_LATENCY_MESSAGES = App.METER.counterBuilder("messages_read_latency")
                                                                          .build();
        private static final LongCounter APPENDED_MESSAGES = App.METER.counterBuilder("messages_appended_total")
                                                                      .build();

        private final PipeId id;
        private final Attributes attributes;
        private final ConcurrentLinkedQueue<Message> messages = new ConcurrentLinkedQueue<>();

        public SimulatedPipe(PipeId id) {
            this.id = id;
            attributes = Attributes.of(stringKey("pipe"), id.id());
        }

        @Override
        public PipeId id() {
            return id;
        }

        @Override
        public void consume(Message message) {
            messages.add(message);
            updateMetrics();
            APPENDED_MESSAGES.add(1, attributes);
        }

        @Override
        public int enqueued() {
            return messages.size();
        }

        @Override
        public Message nextMessage() {
            Optional<Message> msg;
            while((msg = tryNextMessage()).isEmpty()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return msg.get();
        }

        @Override
        public Optional<Message> tryNextMessage() {
            if(messages.isEmpty()) {
                return Optional.empty();
            }

            var msg = messages.poll();

            updateMetrics();
            var withOrigin = attributes.toBuilder().put("origin", msg.origin().id()).build();
            READ_MESSAGES.add(1, withOrigin);
            var latency = msg.timestamp().until(Instant.now()).toMillis();
            READ_LATENCY_MESSAGES.add(latency, withOrigin);

            return Optional.of(msg);
        }

        private void updateMetrics() {
            MESSAGE_COUNT.set(messages.size(), attributes);
        }
    }

    public record SimMessage(PipeId origin, Instant timestamp) implements Message {
    }
}
