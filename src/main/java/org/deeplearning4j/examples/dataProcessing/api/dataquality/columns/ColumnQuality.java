package org.deeplearning4j.examples.dataProcessing.api.dataquality.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor @Data
public abstract class ColumnQuality implements Serializable {

    protected final long countValid;
    protected final long countInvalid;
    protected final long countMissing;
    protected final long countTotal;


    @Override
    public String toString(){
        return "countValid=" + countValid + " ,countInvalid=" + countInvalid + ", countMissing=" + countMissing
                + ", countTotal=" + countTotal;
    }
}
