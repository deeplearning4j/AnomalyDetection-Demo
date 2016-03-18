package org.deeplearning4j.examples.dataProcessing.api.transform.real;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.writable.Writable;

/**Idea: (x-min)/(max-min)
 *
 */
public class DoubleMinMaxNormalizer extends BaseDoubleTransform {

    protected final double min;
    protected final double max;
    protected final double diff;

    public DoubleMinMaxNormalizer(String columnName, double min, double max) {
        super(columnName);
        this.min = min;
        this.max = max;
        this.diff = max-min;
    }

    public Writable map(Writable writable) {
        Double val = writable.toDouble();
        if(Double.isNaN(val)) return new DoubleWritable(0);
        return new DoubleWritable((val-min)/diff);
    }
}
