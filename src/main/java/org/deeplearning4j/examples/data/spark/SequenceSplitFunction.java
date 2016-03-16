package org.deeplearning4j.examples.data.spark;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.sequence.SequenceSplit;

import java.util.Collection;

/**
 * Created by Alex on 17/03/2016.
 */
@AllArgsConstructor
public class SequenceSplitFunction implements FlatMapFunction<Collection<Collection<Writable>>,Collection<Collection<Writable>>>{

    private final SequenceSplit split;

    @Override
    public Iterable<Collection<Collection<Writable>>> call(Collection<Collection<Writable>> collections) throws Exception {
        return split.split(collections);
    }
}
