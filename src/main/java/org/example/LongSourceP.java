package org.example;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static java.lang.Math.abs;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class LongSourceP extends AbstractProcessor {
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 200;

    private final long keyCount;
    private final long totalCountToEmit;
    private int[] initialItems;

    private int globalProcessorIndex;
    private int totalParallelism;

    private long emittedCount;
    private long nowNanos;
    private long lastCallNanos;
    private long lastReport;
    private long emittedCountAtLastReport;
    private final AppendableTraverser<Long> traverser = new AppendableTraverser<>(1);
    private String name;

    LongSourceP(long keyCount, long totalCountToEmit, int[] initialItems) {
        this.keyCount = keyCount;
        this.totalCountToEmit = totalCountToEmit;
        this.initialItems = initialItems;
    }

    @SuppressWarnings("SameParameterValue")
    public static BatchSource<Long> longSource(String name, int keyCount, long totalCount) {
        return Sources.batchFromProcessor(name, new MetaSupplier(keyCount, totalCount));
    }

    @Override
    protected void init(Context context) {
        name = context.vertexName();
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        lastCallNanos = lastReport = System.nanoTime();
    }

    @Override
    public boolean complete() {
        nowNanos = System.nanoTime();
        detectAndReportHiccup();
        return emitEvents();
    }

    private boolean emitEvents() {
        if (globalItemIndex() >= totalCountToEmit) {
            if (emitFromTraverser(traverser)) {
                reportThroughput("random items");
                return true;
            }
            return false;
        }
        Random rnd = ThreadLocalRandom.current();
        while (emitFromTraverser(traverser) && globalItemIndex() < totalCountToEmit) {
            if (globalItemIndex() % 10_000_000 == 0) {
                System.out.printf("%s emitted %,d items%n", name, globalItemIndex());
            }
            if (initialItems != null) {
                traverser.append((long) initialItems[(int) emittedCount]);
                emittedCount++;
                if (emittedCount == initialItems.length) {
                    initialItems = null;
                    reportThroughput("initial items");
                }
            } else {
                traverser.append(abs(rnd.nextLong() % keyCount));
                emittedCount++;
            }
        }
        return false;
    }

    private long globalItemIndex() {
        return emittedCount * totalParallelism + globalProcessorIndex;
    }

    private void detectAndReportHiccup() {
        long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
        if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
            System.out.printf("*** %s.%02d hiccup: %,d ms%n", name, globalProcessorIndex, millisSinceLastCall);
        }
        lastCallNanos = nowNanos;
    }

    private void reportThroughput(String phaseName) {
        long nanosSinceLastReport = nowNanos - lastReport;
        lastReport = nowNanos;
        long emittedSinceLastReport = emittedCount - emittedCountAtLastReport;
        emittedCountAtLastReport = emittedCount;
        System.out.printf("%s.%02d completed %s phase: %,.0f items/second%n",
                name,
                globalProcessorIndex,
                phaseName,
                emittedSinceLastReport / ((double) nanosSinceLastReport / SECONDS.toNanos(1))
        );
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {
        private final int keyCount;
        private final long totalCount;
        private int localParallelism;
        private int memberCount;
        private int[][][] initialItemArrays;

        MetaSupplier(int keyCount, long totalCount) {
            this.keyCount = keyCount;
            this.totalCount = totalCount;
        }

        @Override
        public void init(Context context) {
            System.out.println("Initializing vertex " + context.vertexName());
            localParallelism = context.localParallelism();
            memberCount = context.memberCount();
            createInitialItemArrays();
        }

        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            try {
                int[][][] initialItemsLocal = initialItemArrays;
                return memberAddr ->
                        new ProcSupplier(initialItemsLocal[addresses.indexOf(memberAddr)], keyCount, totalCount);
            } finally {
                initialItemArrays = null;
            }
        }

        private void createInitialItemArrays() {
            initialItemArrays = new int[memberCount][localParallelism][];
            int totalParallelism = memberCount * localParallelism;
            for (int i = 0; i < totalParallelism; i++) {
                int memberIndex = i / localParallelism;
                int localProcessorIndex = i % localParallelism;
                long initialItemCountForThisProcessor =
                        keyCount / totalParallelism + (i < keyCount % totalParallelism ? 1 : 0);
                initialItemArrays[memberIndex][localProcessorIndex] = new int[(int) initialItemCountForThisProcessor];
            }
            for (int i = 0; i < keyCount; i++) {
                set(initialItemArrays, i, i);
            }
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int i = 0; i < keyCount; i++) {
                int j = rnd.nextInt(keyCount);
                int val1 = get(initialItemArrays, i);
                int val2 = get(initialItemArrays, j);
                set(initialItemArrays, i, val2);
                set(initialItemArrays, j, val1);
            }
        }

        private static int get(int[][][] array, int globalIndex) {
            int[] coords = globalIndexToCoords(array, globalIndex);
            return array[coords[0]][coords[1]][coords[2]];
        }

        private static void set(int[][][] array, int globalIndex, int value) {
            int[] coords = globalIndexToCoords(array, globalIndex);
            array[coords[0]][coords[1]][coords[2]] = value;
        }

        private static int[] globalIndexToCoords(int[][][] array, int globalIndex) {
            int memberCount = array.length;
            int localParallelism = array[0].length;
            int memberIndex = globalIndex % memberCount;
            int localProcessorIndex = (globalIndex / memberCount) % localParallelism;
            int localItemIndex = globalIndex / (memberCount * localParallelism);
            return new int[] { memberIndex, localProcessorIndex, localItemIndex };
        }
    }

    private static class ProcSupplier implements ProcessorSupplier {
        private int[][] initialItemArrays;
        private final int keyCount;
        private final long totalCount;

        public ProcSupplier(int[][] initialItemArraysForMember, int keyCount, long totalCount) {
            this.initialItemArrays = initialItemArraysForMember;
            this.keyCount = keyCount;
            this.totalCount = totalCount;
        }

        @Nonnull @Override
        public List<? extends Processor> get(int count) {
            Preconditions.checkTrue(count == initialItemArrays.length, String.format(
                    "This ProcessorSupplier was supposed to return %d processors, but asked to return %d",
                    initialItemArrays.length, count));
            try {
                return Arrays.stream(initialItemArrays)
                             .map(initialItems -> new LongSourceP(keyCount, totalCount, initialItems))
                             .collect(toList());
            } finally {
                initialItemArrays = null;
            }
        }
    }
}
