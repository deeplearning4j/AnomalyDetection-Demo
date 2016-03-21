package org.deeplearning4j.examples.dataProcessing.api.transform.real;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.writable.Writable;

/**
 *
 */
public class DoubleLog2Normalizer extends BaseDoubleTransform {

    protected static final double log2 = Math.log(2);
    protected final double columnMean;
    protected final double columnMin;
    protected final double scalingFactor;

    public DoubleLog2Normalizer(String columnName, double columnMean, double columnMin, double scalingFactor){
        super(columnName);
        if(Double.isNaN(columnMean) || Double.isInfinite(columnMean)) throw new IllegalArgumentException("Invalid input");
        this.columnMean = columnMean;
        this.columnMin = columnMin;
        this.scalingFactor = scalingFactor;
    }

    public Writable map(Writable writable) {
        double val = writable.toDouble();
        if(Double.isNaN(val)) return new DoubleWritable(0);
        return new DoubleWritable(normMean(val));
    }

    private double log2(double x){
        return Math.log(x) / log2;
    }

    private double normMean(double in){
        return scalingFactor * log2((in-columnMin)/(columnMean-columnMin) + 1);
    }

}
