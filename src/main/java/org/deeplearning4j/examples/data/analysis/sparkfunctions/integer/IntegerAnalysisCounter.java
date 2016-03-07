package org.deeplearning4j.examples.data.analysis.sparkfunctions.integer;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Alex on 7/03/2016.
 */
@AllArgsConstructor @Data
public class IntegerAnalysisCounter implements Serializable {

    private long countZero;
    private long countPositive;
    private long countNegative;

    public IntegerAnalysisCounter(){

    }

    public IntegerAnalysisCounter add(IntegerAnalysisCounter other){
        return new IntegerAnalysisCounter(countZero+other.countZero,
                countPositive + other.countPositive,
                countNegative + other.countNegative);
    }

}
