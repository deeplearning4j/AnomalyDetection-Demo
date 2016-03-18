package org.deeplearning4j.examples.data.spark.transform;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.Transform;

import java.util.Collection;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class SparkTransformFunction implements Function<Collection<Writable>,Collection<Writable>> {

    private final Transform transform;

    @Override
    public Collection<Writable> call(Collection<Writable> v1) throws Exception {
        return transform.map(v1);
    }
}
