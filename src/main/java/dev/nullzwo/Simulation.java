package dev.nullzwo;

import dev.nullzwo.flow.PriotizedRoundRobinFlow;
import dev.nullzwo.flow.RateLimitedFlow;
import dev.nullzwo.flow.RoundRobinFlow;
import dev.nullzwo.pipe.InMemPipe;
import dev.nullzwo.pipe.MetricSinkPipe;
import dev.nullzwo.pipe.MetricSourcePipe;
import dev.nullzwo.pipe.PipeId;
import dev.nullzwo.pipe.SinkPipe;
import dev.nullzwo.pipe.SourcePipe;
import io.opentelemetry.api.common.Attributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Simulation {
    private static final Logger log = LoggerFactory.getLogger(Simulation.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
    private final List<ScheduledFuture<?>> jobs = new ArrayList<>();
    private final List<Flow> flows = new ArrayList<>();

    public void runDynamicThrottledFlow() {
        var slowId = new PipeId("slow-inflow");
        var slowInflow = new InMemPipe(slowId);
        var fastId = new PipeId("fast-inflow");
        var fastInflow = new InMemPipe(fastId);
        var outflow = new InMemPipe(new PipeId("outflow"));

        meassureLag(slowInflow, fastInflow, outflow);
        startScenario(slowInflow, fastInflow, outflow);
        var flow = new RateLimitedFlow(outflow, slowInflow, fastInflow);
        runFlow(flow);

        AtomicInteger rate = new AtomicInteger(10);

        scheduler.schedule(() -> {
            if(outflow.size() == 0 && (slowInflow.size() > 0 || fastInflow.size() > 0)) {
                rate.incrementAndGet();
                log.info("Increment rate: {}", rate.get());
            } else if(outflow.size() > 10) {
                rate.decrementAndGet();
                log.info("Decrement rate: {}", rate.get());
            }
        }, 2, SECONDS);

        scheduler.schedule(() -> {
            if(slowInflow.size() == 0) {
                flow.configure(fastId, new Throughput(rate.get(), ofSeconds(1)));
            } else if(fastInflow.size() == 0) {
                flow.configure(slowId, new Throughput(rate.get(), ofSeconds(1)));
            } else {
                flow.configure(slowId, new Throughput(rate.get() / 2, ofSeconds(1)));
                flow.configure(fastId, new Throughput(rate.get() / 2, ofSeconds(1)));
            }
        }, 100, MILLISECONDS);
    }

    public void runThrottledFlow() {
        var slowId = new PipeId("slow-inflow");
        var slowInflow = new InMemPipe(slowId);
        var fastId = new PipeId("fast-inflow");
        var fastInflow = new InMemPipe(fastId);
        var outflow = new InMemPipe(new PipeId("outflow"));

        meassureLag(slowInflow, fastInflow, outflow);
        startScenario(slowInflow, fastInflow, outflow);
        var flow = new RateLimitedFlow(outflow, slowInflow, fastInflow);
        flow.configure(slowId, new Throughput(5, ofSeconds(1)));
        flow.configure(fastId, new Throughput(5, ofSeconds(1)));
        runFlow(flow);
    }

    public void runRoundRobinFlow() {
        var slowInflow = new InMemPipe(new PipeId("slow-inflow"));
        var fastInflow = new InMemPipe(new PipeId("fast-inflow"));
        var outflow = new InMemPipe(new PipeId("outflow"));

        meassureLag(slowInflow, fastInflow, outflow);
        startScenario(slowInflow, fastInflow, outflow);
        runFlow(new RoundRobinFlow(outflow, outflow, slowInflow, fastInflow));
    }

    public void runPriotizedRoundRobinFlow() {
        var slowInflow = new InMemPipe(new PipeId("slow-inflow"));
        var fastInflow = new InMemPipe(new PipeId("fast-inflow"));
        var outflow = new InMemPipe(new PipeId("outflow"));

        meassureLag(slowInflow, fastInflow, outflow);
        startScenario(slowInflow, fastInflow, outflow);
        runFlow(new PriotizedRoundRobinFlow(outflow, outflow, slowInflow, fastInflow));
    }

    public void runKafkaFlow() {
        var kafkaFactory = new KafkaFactory("localhost:9092");

        var slowInflow = kafkaFactory.createSource("slow-inflow");
        var slowInflowSink = kafkaFactory.createSink("slow-inflow");

        var fastInflow = kafkaFactory.createSource("fast-inflow");
        var fastInflowSink = kafkaFactory.createSink("fast-inflow");

        var outflow = kafkaFactory.createSink("outflow");
        var outflowSource = kafkaFactory.createSource("outflow");

        meassureLag(slowInflow, fastInflow, outflowSource);
        startScenario(slowInflowSink, fastInflowSink, outflowSource);
        runFlow(new RoundRobinFlow(outflow, outflowSource, slowInflow, fastInflow));
    }

    public void runStreamsFlow() {
        var kafkaFactory = new KafkaFactory("localhost:9092");

        var slowInflow = kafkaFactory.createSource("slow-inflow");
        var slowInflowSink = kafkaFactory.createSink("slow-inflow");

        var fastInflow = kafkaFactory.createSource("fast-inflow");
        var fastInflowSink = kafkaFactory.createSink("fast-inflow");

        var outflow = kafkaFactory.createSink("outflow");
        var outflowSource = kafkaFactory.createSource("outflow");

        meassureLag(slowInflow, fastInflow, outflowSource);
        startScenario(slowInflowSink, fastInflowSink, outflowSource);
        runFlow(kafkaFactory.createFlow(outflow, slowInflow, fastInflow));
    }

    private void runFlow(Flow flow) {
        log.info("Starting flow: {}", flow.describe());
        flows.add(flow);
        scheduler.execute(flow::run);
        log.info("Flow started: {}", flow.describe());
    }

    private void startScenario(SinkPipe slowInflow, SinkPipe fastInflow, SourcePipe outflow) {
        simulateBurstInflow(slowInflow, new Throughput(80, ofSeconds(20)));
        simulateConstInflow(fastInflow, new Throughput(6, ofSeconds(1)));
        drain(outflow, new Throughput(10, ofSeconds(1)));
        log.info("scheduled simulation jobs");
    }

    private void simulateConstInflow(SinkPipe sinkPipe, Throughput throughput) {
        var pipe = new MetricSinkPipe(sinkPipe);
        schedule(throughput, () -> pipe.consume(new Message(sinkPipe.id(), Instant.now())));
    }

    private void simulateBurstInflow(SinkPipe sinkPipe, Throughput throughput) {
        var pipe = new MetricSinkPipe(sinkPipe);
        var nanos = throughput.duration().toNanos();
        jobs.add(scheduler.scheduleAtFixedRate(
                () -> {
                    for (int i = 0; i < throughput.messageCount(); i++) {
                        pipe.consume(new Message(sinkPipe.id(), Instant.now()));
                    }
                }, nanos, nanos, NANOSECONDS));
    }

    private void drain(SourcePipe outflow, Throughput throughput) {
        var pipe = new MetricSourcePipe<>(outflow);
        schedule(throughput, pipe::tryNextMessage);
    }

    private void meassureLag(QueueLength... lengths) {
        for (var len : lengths) {
            App.METER.gaugeBuilder("messages_queued").ofLongs().buildWithCallback(m -> {
                m.record(len.size(), Attributes.of(stringKey("pipe"), len.id().id()));
            });
        }
    }

    public void stop() {
        for (var flow : flows) {
            log.info("Stopping flow: {}", flow.describe());
            flow.stop();
            log.info("Stopped flow: {}", flow.describe());
        }
        for (var job : jobs) {
            job.cancel(true);
        }
        log.info("stopped jobs");
        scheduler.shutdownNow();
    }

    private void schedule(Throughput throughput, Runnable runnable) {
        var nsPerOp = throughput.duration().toNanos() / throughput.messageCount();
        jobs.add(scheduler.scheduleAtFixedRate(runnable, 0, nsPerOp, NANOSECONDS));
    }
}
