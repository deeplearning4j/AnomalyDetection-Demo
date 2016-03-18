package org.deeplearning4j.examples.data.spark.quality.real;

import org.apache.spark.api.java.function.Function2;
import org.deeplearning4j.examples.data.api.dataquality.columns.RealQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class RealQualityMergeFunction implements Function2<RealQuality,RealQuality,RealQuality> {
    @Override
    public RealQuality call(RealQuality v1, RealQuality v2) throws Exception {
        return v1.add(v2);
    }
}
