package org.deeplearning4j.examples.data.dataquality.spark.categorical;

import org.apache.spark.api.java.function.Function2;
import org.deeplearning4j.examples.data.dataquality.columns.CategoricalQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class CategoricalQualityMergeFunction implements Function2<CategoricalQuality,CategoricalQuality,CategoricalQuality> {
    @Override
    public CategoricalQuality call(CategoricalQuality v1, CategoricalQuality v2) throws Exception {
        return v1.add(v2);
    }
}
