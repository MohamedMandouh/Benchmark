package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

public class InnerJoin {
    private static final long RANGE = 100_000_000;
    private static final long NUM_KEYS = 50_000_000;


    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        Pipeline p = Pipeline.create();

        BatchStage<Long> left = p.readFrom(longSource(RANGE));
        BatchStage<Long> right = p.readFrom(longSource(NUM_KEYS));

        left.innerHashJoin(right, JoinClause.onKeys(n -> n % NUM_KEYS, n -> n % NUM_KEYS),
                Tuple2::tuple2).filter(e -> (e.getKey() % 1_000_000 == 0))
            .writeTo(Sinks.logger());

        Job job = jet.newJob(p);
        job.join();

    }

    private static BatchSource<Long> longSource(Long end) {
        return SourceBuilder
                .batch("longs", c -> LongStream.range(0, end).boxed().iterator())
                .<Long>fillBufferFn((longs, buf) -> {
                    for (int i = 0; i < 128 && longs.hasNext(); i++) {
                        longs.next();
                        buf.add(ThreadLocalRandom.current().nextLong(end));
                    }
                    if (!longs.hasNext()) {
                        buf.close();
                    }
                })
                .build();
    }
}
