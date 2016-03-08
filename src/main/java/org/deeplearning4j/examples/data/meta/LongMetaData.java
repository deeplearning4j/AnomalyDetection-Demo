package org.deeplearning4j.examples.data.meta;

import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.LongWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.ColumnType;

/**
 * Created by Alex on 5/03/2016.
 */
public class LongMetaData implements ColumnMetaData {

    private long minValue;
    private long maxValue;

    public LongMetaData(){
        this(Long.MIN_VALUE,Long.MAX_VALUE);
    }

    public LongMetaData(long minValue, long maxValue){
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Long;
    }

    @Override
    public boolean isValid(Writable writable) {
        long value;
        if( writable instanceof IntWritable || writable instanceof LongWritable){
            value = writable.toLong();
        } else {
            try {
                value = Long.parseLong(writable.toString());
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return value >= minValue && value <= maxValue;
    }

    @Override
    public String toString(){
        return "LongMetaData(minAllowed=" + minValue + ",maxAllowed="+ maxValue + ")";
    }
}
