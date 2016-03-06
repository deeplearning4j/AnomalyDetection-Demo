package org.deeplearning4j.examples.data.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class RealAnalysis implements ColumnAnalysis {

    private final double min;
    private final double max;
    private final double mean;
    private final double sampleStdev;
    private final double sampleVariance;
    private final long count;
    private double[] histogramBuckets;
    private long[] histogramBucketCounts;

    @Override
    public String toString(){
        return "RealAnalysis(min="+min+",max="+max+",mean="+mean+",sampleStDev="+sampleStdev+
                ",sampleVariance="+sampleVariance+",count="+count+")";
    }

    @Override
    public double getMean(){ return mean; }

}
