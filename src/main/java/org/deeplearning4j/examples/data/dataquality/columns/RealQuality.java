package org.deeplearning4j.examples.data.dataquality.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor @Data
public class RealQuality implements ColumnQuality {

    private final long countMissing;
    private final long countNonReal;
    private final long countNaN;
    private final long countInfinite;
    private final long countTotal;

    public RealQuality add(RealQuality other){
        return new RealQuality(countMissing + other.countMissing,
                countNonReal + other.countNonReal,
                countNaN + other.countNaN,
                countInfinite + other.countInfinite,
                countTotal + other.countTotal);
    }

}
