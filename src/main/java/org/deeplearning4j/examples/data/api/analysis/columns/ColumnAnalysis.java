package org.deeplearning4j.examples.data.api.analysis.columns;

import java.io.Serializable;

/**
 * Created by Alex on 4/03/2016.
 */
public interface ColumnAnalysis extends Serializable {

    double getMean();

    double getMin();

    double getMax();

    long getTotalCount();

}
