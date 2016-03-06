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
    private final long count;
    private double[] histogramBuckets;
    private long[] histogramBucketCounts;

    @Override
    public String toString(){
        return "StringAnalysis(unique="+countUnique+",minLen="+ minLength +",maxLen="+ maxLength +",meanLen="+ meanLength +
                ",sampleStDevLen="+ sampleStdevLength + ",sampleVarianceLen="+ sampleVarianceLength +",count="+count+")";
    }

    @Override
    public double getMean(){ return meanLength; }

}
