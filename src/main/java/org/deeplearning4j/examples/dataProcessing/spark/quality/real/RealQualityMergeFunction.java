package org.deeplearning4j.examples.dataProcessing.spark.quality.real;

import org.apache.spark.api.java.function.Function2;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.DoubleQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class RealQualityMergeFunction implements Function2<DoubleQuality,DoubleQuality,DoubleQuality> {
    @Override
    public DoubleQuality call(DoubleQuality v1, DoubleQuality v2) throws Exception {
        return v1.add(v2);
    }
}
