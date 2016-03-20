package org.deeplearning4j.examples.dataProcessing.api.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

import java.util.Collection;
import java.util.Map;

/**
 * Analysis for categorical columns
 *
 * @author Alex Black
 */
@AllArgsConstructor
@Data
public class CategoricalAnalysis implements ColumnAnalysis {

    private final Map<String, Long> mapOfCounts;


    @Override
    public String toString() {
        return "CategoricalAnalysis(CategoryCounts=" + mapOfCounts + ")";
    }

    @Override
    public long getCountTotal() {
        Collection<Long> counts = mapOfCounts.values();
        long sum = 0;
        for (Long l : counts) sum += l;
        return sum;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Categorical;
    }
}
