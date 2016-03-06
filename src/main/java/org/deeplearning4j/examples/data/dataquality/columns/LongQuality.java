package org.deeplearning4j.examples.data.dataquality.columns;

import lombok.Data;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public class LongQuality extends ColumnQuality {

    private final long countNonLong;

    public LongQuality(){
        this(0,0,0,0,0);
    }

    public LongQuality(long countValid, long countInvalid, long countMissing, long countTotal, long countNonLong){
        super(countValid,countInvalid,countMissing,countTotal);
        this.countNonLong = countNonLong;
    }


    public LongQuality add(LongQuality other){
        return new LongQuality(
                countValid + other.countValid,
                countInvalid + other.countInvalid,
                countMissing + other.countMissing,
                countTotal + other.countTotal,
                countNonLong + other.countNonLong);
    }

    @Override
    public String toString(){
        return "LongQuality(" + super.toString() + ", countNonLong=" + countNonLong + ")";
    }

}
