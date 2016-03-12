package org.deeplearning4j.examples.data.analysis.sparkfunctions;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.canova.api.writable.Writable;

import java.util.Collection;

/**
 */
@AllArgsConstructor
public class SelectSequenceFunction implements FlatMapFunction<Collection<Collection<Writable>>, Collection<Writable>> {

    @Override
    public Iterable<Collection<Writable>> call(Collection<Collection<Writable>> writables) throws Exception {
        return writables;

    }

}
