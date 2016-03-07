package org.deeplearning4j.examples.data.analysis.sparkfunctions.real;

import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;

/**
 * Created by Alex on 7/03/2016.
 */
public class RealAnalysisAddFunction implements Function2<RealAnalysisCounter,Writable,RealAnalysisCounter> {

    @Override
    public RealAnalysisCounter call(RealAnalysisCounter v1, Writable writable) throws Exception {

        double value = writable.toDouble();
        long zero = v1.getCountZero();
        long pos = v1.getCountPositive();
        long neg = v1.getCountNegative();

        if(value == 0.0) zero++;
        else if(value < 0.0) neg++;
        else pos++;

        return new RealAnalysisCounter(zero,pos,neg);
    }
}
