package org.deeplearning4j.examples.dataProcessing.spark.quality.integer;

import org.apache.spark.api.java.function.Function2;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.IntegerQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class IntegerQualityMergeFunction implements Function2<IntegerQuality,IntegerQuality,IntegerQuality> {
    @Override
    public IntegerQuality call(IntegerQuality v1, IntegerQuality v2) throws Exception {
        return v1.add(v2);
    }
}
