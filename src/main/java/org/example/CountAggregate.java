package org.example;

import com.hazelcast.jet.pipeline.Sinks;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static org.example.LongSource.longStage;
import static org.example.Main.runJetBenchmark;

public class CountAggregate {

    private static final int NUM_KEYS = 20_000_000;
    private static final long NUM_ITEMS = 4 * NUM_KEYS;

    public static void main(String[] args) {
        runJetBenchmark(p ->
                longStage(p, "input", NUM_KEYS, NUM_ITEMS)
                 .rebalance()
                 .groupingKey(n -> n)
                 .aggregate(counting())
                 .filter(x -> (x.getKey() % (NUM_KEYS / 20)) == 0)
                 .writeTo(Sinks.logger()));
    }
}
