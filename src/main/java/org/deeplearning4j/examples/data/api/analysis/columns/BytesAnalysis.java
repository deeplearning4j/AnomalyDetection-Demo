package org.deeplearning4j.examples.data.api.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class BytesAnalysis implements ColumnAnalysis {


    @Override
    public String toString(){
        return "BytesAnalysis()";
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
        throw new UnsupportedOperationException("Not yet implemented");
    }


}