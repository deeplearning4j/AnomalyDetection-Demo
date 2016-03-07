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

        double newMinValue;
        long countMinValue = v1.getCountMinValue();
        if(value == v1.getMinValueSeen()){
            newMinValue = value;
            countMinValue++;
        } else if(v1.getMinValueSeen() > value){
            newMinValue = value;
            countMinValue = 1;
        } else {
            newMinValue = v1.getMinValueSeen();
            //no change to count
        }

        double newMaxValue;
        long countMaxValue = v1.getCountMaxValue();
        if(value == v1.getMaxValueSeen()){
            newMaxValue = value;
            countMaxValue++;
        } else if(v1.getMaxValueSeen() < value){
            //reset max counter
            newMaxValue = value;
            countMaxValue = 1;
        } else {
            newMaxValue = v1.getMaxValueSeen();
            //no change to count
        }

        return new RealAnalysisCounter(zero,pos,neg,countMinValue,newMinValue,countMaxValue,newMaxValue);
    }
}
