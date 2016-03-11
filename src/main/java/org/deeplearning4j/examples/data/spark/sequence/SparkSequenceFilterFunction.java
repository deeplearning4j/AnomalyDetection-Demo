package org.deeplearning4j.examples.data.spark.sequence;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Filter;

import java.util.Collection;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class SparkSequenceFilterFunction implements Function<Collection<Collection<Writable>>,Boolean> {

    private final Filter filter;

    @Override
    public Boolean call(Collection<Collection<Writable>> v1) throws Exception {
        return !filter.removeSequence(v1);   //Spark: return true to keep example (Filter: return true to remove)
    }
}
