package org.deeplearning4j.examples.data.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class CategoricalAnalysis implements ColumnAnalysis{

    private final Map<String,Long> mapOfCounts;


    @Override
    public String toString() {
        return "CategoricalAnalysis(CategoryCounts=" + mapOfCounts + ")";
    }

    @Override
    public double getMean() { return 0.0;}

}
