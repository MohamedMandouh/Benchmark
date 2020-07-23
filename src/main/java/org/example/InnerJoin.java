package org.example;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Sinks;

import static org.example.LongSource.longStage;
import static org.example.Main.runJetBenchmark;

public class InnerJoin {

    private static final int NUM_KEYS = 25_000_000;
    private static final long NUM_ITEMS = 2 * NUM_KEYS;

    public static void main(String[] args) {
        runJetBenchmark(p -> {
            BatchStage<Long> left = longStage(p, "left", NUM_KEYS, NUM_ITEMS);
            BatchStage<Long> right = longStage(p, "right", NUM_KEYS, NUM_KEYS);
            left.innerHashJoin(right, JoinClause.onKeys(k -> k, k -> k), Tuple2::tuple2)
                .filter(e -> (e.getKey() % (NUM_KEYS / 20) == 0))
                .writeTo(Sinks.logger());
        });
    }
}
