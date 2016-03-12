package org.deeplearning4j.examples.data.analysis.sparkfunctions.seqlength;

import org.apache.spark.api.java.function.DoubleFunction;

/**
 * Created by Alex on 12/03/2016.
 */
public class IntToDoubleFunction implements DoubleFunction<Integer> {
    @Override
    public double call(Integer integer) throws Exception {
        return integer.doubleValue();
    }
}
