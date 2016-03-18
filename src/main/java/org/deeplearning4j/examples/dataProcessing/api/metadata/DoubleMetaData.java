package org.deeplearning4j.examples.dataProcessing.api.metadata;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

/**
 * Created by Alex on 5/03/2016.
 */
public class DoubleMetaData implements ColumnMetaData {

    private final double min;
    private final double max;
    private final boolean allowNaN;
    private final boolean allowInfinite;

    public DoubleMetaData(){
        this(-Double.MAX_VALUE,Double.MAX_VALUE,false,false);
    }

    public DoubleMetaData(double min, double max, boolean allowNaN, boolean allowInfinite){
        this.min = min;
        this.max = max;
        this.allowNaN = allowNaN;
        this.allowInfinite = allowInfinite;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Double;
    }

    @Override
    public boolean isValid(Writable writable) {
        double d;
        try{
            d = writable.toDouble();
        }catch(Exception e){
            return false;
        }

        if(allowNaN && Double.isNaN(d)) return true;
        if(allowInfinite && Double.isInfinite(d)) return true;

        return d >= min && d <= max;
    }

    @Override
    public String toString(){
        return "DoubleMetaData(minAllowed=" + min + ",maxAllowed="+ max + ",allowNaN=" + allowNaN + ",allowInfinite="+ allowInfinite + ")";
    }
}
