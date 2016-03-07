package org.deeplearning4j.examples.data.analysis.sparkfunctions.real;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Alex on 7/03/2016.
 */
@AllArgsConstructor @Data
public class RealAnalysisCounter implements Serializable {

    private long countZero;
    private long countPositive;
    private long countNegative;

    public RealAnalysisCounter(){

    }

    public RealAnalysisCounter add(RealAnalysisCounter other){
        return new RealAnalysisCounter(countZero+other.countZero,
                countPositive + other.countPositive,
                countNegative + other.countNegative);
    }

}
