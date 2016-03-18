package org.deeplearning4j.examples.data.api.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data @Builder
public class LongAnalysis implements ColumnAnalysis{

    private final long min;
    private final long max;
    private final double mean;
    private final double sampleStdev;
    private final double sampleVariance;
    private final long countZero;
    private final long countNegative;
    private final long countPositive;
    private final long countMinValue;
    private final long countMaxValue;
    private final long countTotal;
    private final double[] histogramBuckets;
    private final long[] histogramBucketCounts;

    @Override
    public String toString(){
        return "LongAnalysis(min="+min+",max="+max+",mean="+mean+",sampleStDev="+sampleStdev+
                ",sampleVariance="+sampleVariance+",countZero="+countZero + ",countNegative="+countNegative
                +",countPositive="+countPositive+",countMinValue="+countMinValue+",countMaxValue="+countMaxValue+
                ",countTotal="+ countTotal +")";
    }

    @Override
    public double getMean(){ return mean; }

    @Override
    public double getMin(){
        return min;
    }

    @Override
    public double getMax(){
        return max;
    }

    @Override
    public long getTotalCount() {
        return countTotal;
    }

}
