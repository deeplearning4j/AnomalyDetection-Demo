package org.deeplearning4j.examples.data.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class IntegerAnalysis implements ColumnAnalysis{

    private final int min;
    private final int max;
    private final double mean;
    private final double sampleStdev;
    private final double sampleVariance;
    private final long count;
    private final double[] histogramBuckets;
    private final long[] histogramBucketCounts;

    @Override
    public String toString(){
        return "IntegerAnalysis(min="+min+",max="+max+",mean="+mean+",sampleStDev="+sampleStdev+
                ",sampleVariance="+sampleVariance+",count="+count+")";
    }

}
