package org.deeplearning4j.examples.data.api.metadata;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.ColumnType;

/**
 * Created by Alex on 5/03/2016.
 */
public class IntegerMetaData implements ColumnMetaData {

    private int minValue;
    private int maxValue;

    public IntegerMetaData(){
        this(Integer.MIN_VALUE,Integer.MAX_VALUE);
    }

    public IntegerMetaData(int minValue, int maxValue){
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Integer;
    }

    @Override
    public boolean isValid(Writable writable) {
        int value;
        try{
            value = Integer.parseInt(writable.toString());
        }catch(NumberFormatException e){
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    @Override
    public String toString(){
        return "IntegerMetaData(minAllowed=" + minValue + ",maxAllowed="+ maxValue + ")";
    }
}
