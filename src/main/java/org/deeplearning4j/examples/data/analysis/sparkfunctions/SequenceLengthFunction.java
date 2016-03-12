package org.deeplearning4j.examples.data.analysis.sparkfunctions;

import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;

import java.util.Collection;

/**
 * Created by Alex on 12/03/2016.
 */
public class SequenceLengthFunction implements Function<Collection<Collection<Writable>>,Integer> {
    @Override
    public Integer call(Collection<Collection<Writable>> v1) throws Exception {
        return v1.size();
    }
}
