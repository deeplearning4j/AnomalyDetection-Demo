package org.deeplearning4j.examples.data.spark.analysis.real;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Alex on 7/03/2016.
 */
@AllArgsConstructor @Data
public class RealAnalysisCounter implements Serializable {

    private long countZero;
    private long countPositive;
    private long countNegative;
    private long countMinValue;
    private double minValueSeen = Double.MAX_VALUE;
    private long countMaxValue;
    private double maxValueSeen = -Double.MIN_VALUE;

    public RealAnalysisCounter(){

    }

    public RealAnalysisCounter add(RealAnalysisCounter other){

        double otherMin = other.getMinValueSeen();
        double newMinValueSeen;
        long newCountMinValue;
        if(minValueSeen == otherMin){
            newMinValueSeen = minValueSeen;
            newCountMinValue = countMinValue + other.countMinValue;
        } else if(minValueSeen > otherMin) {
            //Keep other, take count from other
            newMinValueSeen = otherMin;
            newCountMinValue = other.countMinValue;
        } else {
            //Keep this min, no change to count
            newMinValueSeen = minValueSeen;
            newCountMinValue = countMinValue;
        }

        double otherMax = other.getMaxValueSeen();
        double newMaxValueSeen;
        long newCountMaxValue;
        if(maxValueSeen == otherMax){
            newMaxValueSeen = maxValueSeen;
            newCountMaxValue = countMaxValue + other.countMaxValue;
        } else if(maxValueSeen < otherMax) {
            //Keep other, take count from other
            newMaxValueSeen = otherMax;
            newCountMaxValue = other.countMaxValue;
        } else {
            //Keep this max, no change to count
            newMaxValueSeen = maxValueSeen;
            newCountMaxValue = countMaxValue;
        }

        return new RealAnalysisCounter(countZero+other.countZero,
                countPositive + other.countPositive,
                countNegative + other.countNegative,
                newCountMinValue,
                newMinValueSeen,
                newCountMaxValue,
                newMaxValueSeen);
    }

}