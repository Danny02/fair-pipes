package dev.nullzwo;

import io.vavr.collection.Map;
import io.vavr.collection.Seq;

import static java.time.Duration.ofSeconds;

public class ThroughputReconciliation {

    public Map<PipeId, Throughput> reconcile(Seq<SourcePipe> inputs) {
        return inputs.toMap(SourcePipe::id, _ -> new Throughput(10, ofSeconds(1)));
        // is system limited (is percentage of load needed?)

        // what systems want to push data
        // what has priority
    }
}
