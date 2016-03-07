package org.deeplearning4j.examples.data.analysis.sparkfunctions.integer;

import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;

/**
 * Created by Alex on 7/03/2016.
 */
public class IntegerAnalysisAddFunction implements Function2<IntegerAnalysisCounter,Writable,IntegerAnalysisCounter> {

    @Override
    public IntegerAnalysisCounter call(IntegerAnalysisCounter v1, Writable writable) throws Exception {

        int value = writable.toInt();
        long zero = v1.getCountZero();
        long pos = v1.getCountPositive();
        long neg = v1.getCountNegative();

        if(value == 0) zero++;
        else if(value < 0) neg++;
        else pos++;

        int newMinValue;
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

        int newMaxValue;
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

        return new IntegerAnalysisCounter(zero,pos,neg,countMinValue,newMinValue,countMaxValue,newMaxValue);
    }
}
