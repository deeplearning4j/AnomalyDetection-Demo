package org.deeplearning4j.examples.dataProcessing.spark.analysis.longa;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by Alex on 5/03/2016.
 */
public class LongAnalysisMergeFunction implements Function2<LongAnalysisCounter,LongAnalysisCounter,LongAnalysisCounter> {
    @Override
    public LongAnalysisCounter call(LongAnalysisCounter v1, LongAnalysisCounter v2) throws Exception {
        return v1.add(v2);
    }
}
