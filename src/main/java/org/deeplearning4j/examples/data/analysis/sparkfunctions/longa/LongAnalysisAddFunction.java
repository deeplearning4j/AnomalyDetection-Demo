package org.deeplearning4j.examples.data.analysis.sparkfunctions.longa;

import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;

/**
 * Created by Alex on 7/03/2016.
 */
public class LongAnalysisAddFunction implements Function2<LongAnalysisCounter,Writable,LongAnalysisCounter> {

    @Override
    public LongAnalysisCounter call(LongAnalysisCounter v1, Writable writable) throws Exception {

        long value = writable.toLong();
        long zero = v1.getCountZero();
        long pos = v1.getCountPositive();
        long neg = v1.getCountNegative();

        if(value == 0) zero++;
        else if(value < 0) neg++;
        else pos++;

        return new LongAnalysisCounter(zero,pos,neg);
    }
}
