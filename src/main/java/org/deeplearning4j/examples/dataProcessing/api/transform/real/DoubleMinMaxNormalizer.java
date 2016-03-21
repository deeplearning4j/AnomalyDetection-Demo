package org.deeplearning4j.examples.dataProcessing.api.transform.real;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.writable.Writable;

/**
 * Normalizer to map (min to max) -> (newMin-to newMax) linearly. <br>
 *
 * Mathematically: (newMax-newMin)/(max-min) * (x-min) + newMin
 *
 * @author Alex Black
 */
public class DoubleMinMaxNormalizer extends BaseDoubleTransform {

    protected final double min;
    protected final double max;
    protected final double newMin;
    protected final double newMax;
    protected final double ratio;

    public DoubleMinMaxNormalizer(String columnName, double min, double max){
        this(columnName, min, max, 0, 1);
    }

    public DoubleMinMaxNormalizer(String columnName, double min, double max, double newMin, double newMax) {
        super(columnName);
        this.min = min;
        this.max = max;
        this.newMin = newMin;
        this.newMax = newMax;
        this.ratio = (newMax-newMin) / (max-min);
    }

    public Writable map(Writable writable) {
        double val = writable.toDouble();
        if(Double.isNaN(val)) return new DoubleWritable(0);
        return new DoubleWritable(ratio*(val-min) + newMin);
    }
}
