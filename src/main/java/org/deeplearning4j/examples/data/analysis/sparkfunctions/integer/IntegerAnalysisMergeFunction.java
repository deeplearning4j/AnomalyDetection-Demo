package org.deeplearning4j.examples.data.analysis.sparkfunctions.integer;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by Alex on 5/03/2016.
 */
public class IntegerAnalysisMergeFunction implements Function2<IntegerAnalysisCounter,IntegerAnalysisCounter,IntegerAnalysisCounter> {
    @Override
    public IntegerAnalysisCounter call(IntegerAnalysisCounter v1, IntegerAnalysisCounter v2) throws Exception {
        return v1.add(v2);
    }
}
