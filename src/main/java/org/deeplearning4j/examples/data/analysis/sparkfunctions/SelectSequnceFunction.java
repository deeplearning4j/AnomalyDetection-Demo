package org.deeplearning4j.examples.data.analysis.sparkfunctions;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;

import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor
public class SelectSequnceFunction implements Function<Collection<Collection<Writable>>,Collection<Writable>> {

    private final int timestep;

    @Override
    public Collection<Writable> call(Collection<Collection<Writable>> writables) throws Exception {
        return ((List<Collection<Writable>>) writables).get(timestep);

    }

}
