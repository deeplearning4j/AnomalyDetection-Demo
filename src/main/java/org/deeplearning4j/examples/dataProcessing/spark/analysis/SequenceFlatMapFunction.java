package org.deeplearning4j.examples.dataProcessing.spark.analysis;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.canova.api.writable.Writable;

import java.util.Collection;

/**
 */

public class SequenceFlatMapFunction implements FlatMapFunction<Collection<Collection<Writable>>, Collection<Writable>> {
    @Override
    public Iterable<Collection<Writable>> call(Collection<Collection<Writable>> collections) throws Exception {
        return collections;
    }

}
