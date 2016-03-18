package org.deeplearning4j.examples.data.api.dataquality.columns;

/**
 * Created by Alex on 5/03/2016.
 */
public class BytesQuality extends ColumnQuality {

    public BytesQuality(){
        this(0,0,0,0);
    }

    public BytesQuality(long countValid, long countInvalid, long countMissing, long countTotal){
        super(countValid,countInvalid,countMissing,countTotal);
    }

    @Override
    public String toString(){
        return "BytesQuality(" + super.toString() + ")";
    }

}
