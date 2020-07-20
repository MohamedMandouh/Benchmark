package org.example;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Sinks;

import static org.example.LongSource.longStage;
import static org.example.Main.runJetBenchmark;

public class InnerJoin {

    private static final long NUM_KEYS = 50_000_000;
    private static final long RANGE = 100_000_000;

    public static void main(String[] args) {
        runJetBenchmark(p -> {
            BatchStage<Long> left = longStage(p, "left", NUM_KEYS, RANGE);
            BatchStage<Long> right = longStage(p, "right", NUM_KEYS);
            left.innerHashJoin(right, JoinClause.onKeys(n -> n % NUM_KEYS, n -> n % NUM_KEYS),
                    Tuple2::tuple2).filter(e -> (e.getKey() % 1_000_000 == 0))
                .writeTo(Sinks.logger());

        });
    }
}
