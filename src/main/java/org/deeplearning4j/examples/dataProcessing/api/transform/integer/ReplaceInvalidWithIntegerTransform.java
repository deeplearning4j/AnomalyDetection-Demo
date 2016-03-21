package org.deeplearning4j.examples.dataProcessing.api.transform.integer;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;

/**
 * Replace an invalid (non-integer) value in a column with a specified integer
 */
public class ReplaceInvalidWithIntegerTransform extends BaseIntegerTransform {

    private final int intValue;

    public ReplaceInvalidWithIntegerTransform(String column, int intValue) {
        super(column);
        this.intValue = intValue;
    }

    @Override
    public Writable map(Writable writable) {
        if(inputSchema.getMetaData(columnNumber).isValid(writable)){
            return writable;
        } else {
            return new IntWritable(intValue);
        }
    }
}
