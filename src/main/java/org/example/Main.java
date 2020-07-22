package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;

import java.util.function.Consumer;

class Main {
    static void runJetBenchmark(Consumer<? super Pipeline> buildPipelineFn) {
        Pipeline p = Pipeline.create();
        buildPipelineFn.accept(p);
        JetInstance jet = Jet.bootstrappedInstance();
        try {
            jet.newJob(p).join();
        } finally{
            Jet.shutdownAll();
        }
    }
}
