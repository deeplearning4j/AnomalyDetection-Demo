package org.deeplearning4j.examples.data.api.transform.integer;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;

/**
 * Created by Alex on 6/03/2016.
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
