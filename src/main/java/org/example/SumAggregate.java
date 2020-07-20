package org.example;

import com.hazelcast.jet.pipeline.Sinks;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static org.example.LongSource.longStage;
import static org.example.Main.runJetBenchmark;

public class SumAggregate {

    private static final long NUM_KEYS = 50_000_000;
    private static final long RANGE = 100_000_000;

    public static void main(String[] args) {
        runJetBenchmark(p ->
                longStage(p, "input", NUM_KEYS, RANGE)
                        .groupingKey(n -> n)
                        .aggregate(summingLong(n -> n))
                        .filter(x -> (x.getKey() % NUM_KEYS) == 1_000_000)
                        .writeTo(Sinks.logger()));
    }
}
