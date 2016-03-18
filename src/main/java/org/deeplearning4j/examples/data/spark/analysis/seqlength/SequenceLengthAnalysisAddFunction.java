package org.deeplearning4j.examples.data.spark.analysis.seqlength;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by Alex on 7/03/2016.
 */
public class SequenceLengthAnalysisAddFunction implements Function2<SequenceLengthAnalysisCounter,Integer,SequenceLengthAnalysisCounter> {

    @Override
    public SequenceLengthAnalysisCounter call(SequenceLengthAnalysisCounter v1, Integer length) throws Exception {

        long zero = v1.getCountZeroLength();
        long one = v1.getCountOneLength();

        if(length == 0) zero++;
        else if(length == 1) one++;

        int newMinValue;
        long countMinValue = v1.getCountMinLength();
        if(length == v1.getMinLengthSeen()){
            newMinValue = length;
            countMinValue++;
        } else if(v1.getMinLengthSeen() > length){
            newMinValue = length;
            countMinValue = 1;
        } else {
            newMinValue = v1.getMinLengthSeen();
            //no change to count
        }

        int newMaxValue;
        long countMaxValue = v1.getCountMaxLength();
        if(length == v1.getMaxLengthSeen()){
            newMaxValue = length;
            countMaxValue++;
        } else if(v1.getMaxLengthSeen() < length){
            //reset max counter
            newMaxValue = length;
            countMaxValue = 1;
        } else {
            newMaxValue = v1.getMaxLengthSeen();
            //no change to count
        }

        //New mean:
        double sum = v1.getMean()*v1.getCountTotal() + length;
        long newTotalCount = v1.getCountTotal()+1;
        double newMean = sum / newTotalCount;

        return new SequenceLengthAnalysisCounter(zero,one,countMinValue,newMinValue,countMaxValue,newMaxValue,newTotalCount,newMean);
    }
}
