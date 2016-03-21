package org.deeplearning4j.examples.dataProcessing.spark.analysis;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.canova.api.writable.Writable;

import java.util.List;

/**
 */

public class SequenceFlatMapFunction implements FlatMapFunction<List<List<Writable>>, List<Writable>> {
    @Override
    public Iterable<List<Writable>> call(List<List<Writable>> collections) throws Exception {
        return collections;
    }

}
