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

public class LongSourceWithFlatmap {
    private static final int SOURCE_STEP = 100;

    private final String name;
    private final long countToEmit;
    private final long keyCount;

    private int[] keysToEmit;
    private long emittedCount;
    private long timeMarkNanos;
    private long emittedCountMark;

    LongSourceWithFlatmap(String name, long keyCount, long countToEmit) {
        Preconditions.checkTrue(keyCount < Integer.MAX_VALUE, "Too many keys, maximum is " + Integer.MAX_VALUE);
        Preconditions.checkTrue(keyCount <= countToEmit, "countToEmit must be at least as much as keyCount");
        Preconditions.checkTrue(keyCount % SOURCE_STEP == 0, "keyCount must be a multiple of " + SOURCE_STEP);
        Preconditions.checkTrue(countToEmit % SOURCE_STEP == 0, "countToEmit must be a multiple of " + SOURCE_STEP);
        this.name = name;
        this.countToEmit = countToEmit;
        this.keyCount = keyCount;
        this.keysToEmit = shuffledKeys((int) keyCount);
    }

    static BatchStage<Long> longStageWithFlatmap(Pipeline p, String sourceName, int keyCount, long totalCount) {
        return p.readFrom(longSourceWithFlatmap(sourceName, keyCount, totalCount))
                .flatMap(n -> {
                    Long[] array = new Long[SOURCE_STEP];
                    Arrays.setAll(array, i -> n + i);
                    return traverseArray(array);
                });
    }

    static BatchStage<Long> longStageWithFlatmap(Pipeline p, String sourceName, int count) {
        return longStageWithFlatmap(p, sourceName, count, count);
    }

    private static BatchSource<Long> longSourceWithFlatmap(String name, int keyCount, long totalCount) {
        Preconditions.checkTrue(keyCount <= totalCount, "countToEmit must be at least as much as keyCount");
        return SourceBuilder
                .batch(name, c -> new LongSourceWithFlatmap(c.vertexName(), keyCount, totalCount))
                .fillBufferFn(LongSourceWithFlatmap::fillBuffer)
                .build();
    }

    void fillBuffer(SourceBuffer<? super Long> buf) {
        if (emittedCount == 0) {
            timeMarkNanos = System.nanoTime();
        }
        Random rnd = ThreadLocalRandom.current();
        for (int i = 0; i < 256 && emittedCount < countToEmit; i++, emittedCount += SOURCE_STEP) {
            if (emittedCount % 10_000_000 == 0) {
                System.out.printf("Emitted %,d%n", emittedCount);
            }
            if (emittedCount == keyCount) {
                keysToEmit = null;
                reportThroughput("distinct keys");
            }
            long itemToEmit = keysToEmit != null
                    ? keysToEmit[(int) emittedCount / SOURCE_STEP]
                    : rnd.nextInt((int) keyCount / SOURCE_STEP) * SOURCE_STEP;
            buf.add(itemToEmit);
        }
        if (emittedCount == countToEmit) {
            buf.close();
            reportThroughput("random keys");
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
        int[] shuffledKeys = new int[keyCount / SOURCE_STEP];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Arrays.setAll(shuffledKeys, i -> i * SOURCE_STEP);
        for (int i = 0; i < shuffledKeys.length; i++) {
            int swapLocation = rnd.nextInt(shuffledKeys.length);
            int tmp = shuffledKeys[i];
            shuffledKeys[i] = shuffledKeys[swapLocation];
            shuffledKeys[swapLocation] = tmp;
        }
        return shuffledKeys;
    }
}
