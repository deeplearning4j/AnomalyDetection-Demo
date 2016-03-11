package org.deeplearning4j.examples.data.dataquality.columns;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Created by Alex on 5/03/2016.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class IntegerQuality extends ColumnQuality {

    private final long countNonInteger;

    public IntegerQuality(long countValid, long countInvalid, long countMissing, long countTotal, long countNonInteger){
        super(countValid,countInvalid,countMissing,countTotal);
        this.countNonInteger = countNonInteger;
    }


    public IntegerQuality add(IntegerQuality other){
        return new IntegerQuality(
                countValid + other.countValid,
                countInvalid + other.countInvalid,
                countMissing + other.countMissing,
                countTotal + other.countTotal,
                countNonInteger + other.countNonInteger);
    }

    @Override
    public String toString(){
        return "IntegerQuality(" + super.toString() + ", countNonInteger=" + countNonInteger + ")";
    }

}
