package org.deeplearning4j.examples.data.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class StringAnalysis implements ColumnAnalysis {

    private final long countUnique;
    private final int minLength;
    private final int maxLength;
    private final double meanLength;
    private final double sampleStdevLength;
    private final double sampleVarianceLength;
    private final long countTotal;
    private double[] histogramBuckets;
    private long[] histogramBucketCounts;

    @Override
    public String toString(){
        return "StringAnalysis(unique="+countUnique+",minLen="+ minLength +",maxLen="+ maxLength +",meanLen="+ meanLength +
                ",sampleStDevLen="+ sampleStdevLength + ",sampleVarianceLen="+ sampleVarianceLength +",count="+ countTotal +")";
    }

    @Override
    public double getMean(){ return meanLength; }

    @Override
    public double getMin() {
        return minLength;
    }

    @Override
    public double getMax() {
        return maxLength;
    }

    @Override
    public long getTotalCount() {
        return countTotal;
    }

}
