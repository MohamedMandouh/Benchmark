package org.example;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.Traversers.traverseArray;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LongSource {
    private final String name;
    private final long countToEmit;
    private final long keyCount;

    private int[] keysToEmit;
    private long emittedCount;
    private long timeMarkNanos;
    private long emittedCountMark;

    LongSource(String name, long keyCount, long countToEmit) {
        Preconditions.checkTrue(keyCount < Integer.MAX_VALUE, "Too many keys, maximum is " + Integer.MAX_VALUE);
        Preconditions.checkTrue(keyCount <= countToEmit, "countToEmit must be at least as much as keyCount");
        this.name = name;
        this.countToEmit = countToEmit;
        this.keyCount = keyCount;
        this.keysToEmit = shuffledKeys((int) keyCount);
    }

    static BatchSource<Long> longSource(String name, long keyCount, long totalCount) {
        return SourceBuilder
                .batch(name, c -> new LongSource(c.vertexName(), keyCount, totalCount))
                .fillBufferFn(LongSource::fillBuffer)
                .build();
    }

    void fillBuffer(SourceBuffer<? super Long> buf) {
        if (emittedCount == 0) {
            timeMarkNanos = System.nanoTime();
        }
        Random rnd = ThreadLocalRandom.current();
        for (int i = 0; i < 256 && emittedCount < countToEmit; i++, emittedCount++) {
            if (emittedCount % 1_000_000 == 0) {
                System.out.printf("%s emitted %,d%n", name, emittedCount);
            }
            if (emittedCount == keyCount) {
                keysToEmit = null;
                reportThroughput("distinct keys");
            }
            long itemToEmit = keysToEmit != null ? keysToEmit[(int) emittedCount] : rnd.nextInt((int) keyCount);
            buf.add(itemToEmit);
        }
        if (emittedCount == countToEmit) {
            buf.close();
            reportThroughput(emittedCount == keyCount ? "distinct keys" : "random keys");
        }
    }

    private void reportThroughput(String phaseName) {
        long nowNanos = System.nanoTime();
        long nanosSinceStart = nowNanos - timeMarkNanos;
        timeMarkNanos = nowNanos;
        long emittedSinceLastReport = emittedCount - emittedCountMark;
        emittedCountMark = emittedCount;
        System.out.printf("%s %s phase throughput: %,d items/second%n",
                name, phaseName, emittedSinceLastReport * SECONDS.toNanos(1) / nanosSinceStart);
    }

    private static int[] shuffledKeys(int keyCount) {
        int[] shuffledKeys = new int[keyCount];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Arrays.setAll(shuffledKeys, i -> i);
        for (int i = 0; i < shuffledKeys.length; i++) {
            int swapLocation = rnd.nextInt(shuffledKeys.length);
            int tmp = shuffledKeys[i];
            shuffledKeys[i] = shuffledKeys[swapLocation];
            shuffledKeys[swapLocation] = tmp;
        }
        return shuffledKeys;
    }
}
