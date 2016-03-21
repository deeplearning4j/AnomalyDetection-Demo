package org.deeplearning4j.examples.dataProcessing.spark.transform;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceSplit;

import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 17/03/2016.
 */
@AllArgsConstructor
public class SequenceSplitFunction implements FlatMapFunction<List<List<Writable>>,List<List<Writable>>>{

    private final SequenceSplit split;

    @Override
    public Iterable<List<List<Writable>>> call(List<List<Writable>> collections) throws Exception {
        return split.split(collections);
    }
}
