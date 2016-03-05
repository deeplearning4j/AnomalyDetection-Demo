package org.deeplearning4j.examples.data.dataquality.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor @Data
public class IntegerQuality implements ColumnQuality {

    private final long countMissing;
    private final long countNonInteger;
    private final long countTotal;

    public IntegerQuality add(IntegerQuality other){
        return new IntegerQuality(countMissing + other.countMissing,
                countNonInteger + other.countNonInteger,
                countTotal + other.countTotal);
    }

}
