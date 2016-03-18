package org.deeplearning4j.examples.dataProcessing.spark.sequence;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.Transform;

import java.util.Collection;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class SparkSequenceTransformFunction implements Function<Collection<Collection<Writable>>,Collection<Collection<Writable>>> {

    private final Transform transform;

    @Override
    public Collection<Collection<Writable>> call(Collection<Collection<Writable>> v1) throws Exception {
        return transform.mapSequence(v1);
    }
}
