package org.deeplearning4j.examples.dataProcessing.api.analysis.columns;

import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

import java.io.Serializable;

/**
 * Interface for column analysis
 */
public interface ColumnAnalysis extends Serializable {

    long getCountTotal();

    ColumnType getColumnType();

}
