package org.deeplearning4j.examples.data.analysis.sparkfunctions;

import org.apache.spark.api.java.function.DoubleFunction;
import org.canova.api.writable.Writable;

/**
 * Created by Alex on 4/03/2016.
 */
public class StringLengthFunction implements DoubleFunction<Writable> {
    @Override
    public double call(Writable writable) throws Exception {
        return writable.toString().length();
    }
}
