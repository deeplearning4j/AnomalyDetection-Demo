package org.deeplearning4j.examples.dataProcessing.spark.transform;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.Transform;

import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class SparkTransformFunction implements Function<List<Writable>,List<Writable>> {

    private final Transform transform;

    @Override
    public List<Writable> call(List<Writable> v1) throws Exception {
        return transform.map(v1);
    }
}
