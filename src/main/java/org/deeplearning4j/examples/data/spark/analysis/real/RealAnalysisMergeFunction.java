package org.deeplearning4j.examples.data.spark.analysis.real;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by Alex on 5/03/2016.
 */
public class RealAnalysisMergeFunction implements Function2<RealAnalysisCounter,RealAnalysisCounter,RealAnalysisCounter> {
    @Override
    public RealAnalysisCounter call(RealAnalysisCounter v1, RealAnalysisCounter v2) throws Exception {
        return v1.add(v2);
    }
}
