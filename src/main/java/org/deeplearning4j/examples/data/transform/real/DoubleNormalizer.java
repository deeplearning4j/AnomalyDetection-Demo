package org.deeplearning4j.examples.data.transform.real;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.writable.Writable;

/**
 *
 */

public class DoubleNormalizer extends BaseDoubleTransform{

    protected static final double log2 = Math.log(2);
    protected final double columnMean;
    protected final double scalingFactor;
    protected final boolean norm0;

    public DoubleNormalizer(String columnName, double columnMean, double scalingFactor) {
        this(columnName, columnMean, scalingFactor, true);
    }

    public DoubleNormalizer(String columnName, double columnMean, double scalingFactor, boolean norm0) {
        super(columnName);
        if(Double.isNaN(columnMean)) throw new IllegalArgumentException("Invalid input");
        this.columnMean = columnMean;
        this.scalingFactor = scalingFactor;
        this.norm0 = norm0;
    }

    public Writable map(Writable writable) {
        Double val = writable.toDouble();
        if(Double.isNaN(val)) return new DoubleWritable(0);
        if(norm0) return new DoubleWritable(norm0(columnMean, val));
        return new DoubleWritable(norm1(columnMean, val));
    }

    private double log2(double x){
        return Math.log(x) / log2;
    }

    private double norm0(double mean, double in){
        if(mean == 0.0) return 0.0;
        return scalingFactor * log2(in/mean + 1);
    }

    private double norm1(double mean, double in){
        return scalingFactor * log2((in-1)/(mean-1) + 1);
    }

}
