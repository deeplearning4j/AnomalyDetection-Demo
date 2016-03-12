package org.deeplearning4j.examples.data.analysis.sparkfunctions.seqlength;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by Alex on 5/03/2016.
 */
public class SequenceLengthAnalysisMergeFunction implements Function2<SequenceLengthAnalysisCounter,SequenceLengthAnalysisCounter,SequenceLengthAnalysisCounter> {
    @Override
    public SequenceLengthAnalysisCounter call(SequenceLengthAnalysisCounter v1, SequenceLengthAnalysisCounter v2) throws Exception {
        return v1.add(v2);
    }
}
