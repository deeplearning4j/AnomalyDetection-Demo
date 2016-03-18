package org.deeplearning4j.examples.data.api.dataquality.columns;

/**
 * Created by Alex on 5/03/2016.
 */
public class BlobQuality extends ColumnQuality {

    public BlobQuality(){
        this(0,0,0,0);
    }

    public BlobQuality(long countValid, long countInvalid, long countMissing, long countTotal){
        super(countValid,countInvalid,countMissing,countTotal);
    }

    @Override
    public String toString(){
        return "BlobQuality(" + super.toString() + ")";
    }

}
