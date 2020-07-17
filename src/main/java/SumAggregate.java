import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;

public class SumAggregate {
    private static final long RANGE = 100_000_000;
    private static final long NUM_KEYS = 50_000_000;


    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        Pipeline p = Pipeline.create();

        p.readFrom(longSource(RANGE))
         .groupingKey(n -> n % NUM_KEYS)
         .aggregate(summingLong(n -> n))
         .filter(x -> (x.getKey() % NUM_KEYS) == 1_000_000)
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
