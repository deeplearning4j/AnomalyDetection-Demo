package org.deeplearning4j.examples.dataProcessing.api.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collection;
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
    public double getMean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getMin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getMax() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalCount() {
        Collection<Long> counts = mapOfCounts.values();
        long sum = 0;
        for(Long l : counts) sum += l;
        return sum;
    }

}
