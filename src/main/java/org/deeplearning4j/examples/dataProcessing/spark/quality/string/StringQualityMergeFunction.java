package org.deeplearning4j.examples.dataProcessing.spark.quality.string;

import org.apache.spark.api.java.function.Function2;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.StringQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class StringQualityMergeFunction implements Function2<StringQuality,StringQuality,StringQuality> {
    @Override
    public StringQuality call(StringQuality v1, StringQuality v2) throws Exception {
        return v1.add(v2);
    }
}
