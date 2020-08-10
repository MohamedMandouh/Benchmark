package org.example;

import com.hazelcast.jet.pipeline.Sinks;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static org.example.LongSource.longStage;
import static org.example.Main.runJetBenchmark;

public class Sort {

    private static final int NUM_KEYS = 1_000_000;
    private static final long NUM_ITEMS = NUM_KEYS;

    public static void main(String[] args) {
        runJetBenchmark(p ->
                longStage(p, "input", NUM_KEYS, NUM_ITEMS)
                        .sort(n -> n)
                        .filter(n -> n % 1_000 == 0)
                        .writeTo(Sinks.logger()));
    }
}
