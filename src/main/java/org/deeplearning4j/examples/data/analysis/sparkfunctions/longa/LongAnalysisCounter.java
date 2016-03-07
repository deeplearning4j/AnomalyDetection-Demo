package org.deeplearning4j.examples.data.analysis.sparkfunctions.longa;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Alex on 7/03/2016.
 */
@AllArgsConstructor @Data
public class LongAnalysisCounter implements Serializable {

    private long countZero;
    private long countPositive;
    private long countNegative;

    public LongAnalysisCounter(){

    }

    public LongAnalysisCounter add(LongAnalysisCounter other){
        return new LongAnalysisCounter(countZero+other.countZero,
                countPositive + other.countPositive,
                countNegative + other.countNegative);
    }

}
