package org.example;

import com.hazelcast.jet.pipeline.Sinks;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static org.example.LongSourceP.longSource;
import static org.example.Main.runJetBenchmark;

public class SumAggregate {

    private static final int NUM_KEYS = 1_000_000;
    private static final long NUM_ITEMS = 2 * NUM_KEYS;

    public static void main(String[] args) {
        runJetBenchmark(p ->
                p.readFrom(longSource("input", NUM_KEYS, NUM_ITEMS))
                 .rebalance()
                 .groupingKey(n -> n)
                 .aggregate(counting())
                 .filter(x -> (x.getKey() % (NUM_KEYS / 20)) == 0)
                 .writeTo(Sinks.logger()));
    }
}
