package org.deeplearning4j.examples.data.analysis.sparkfunctions;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor
public class SelectSequnceColumnFunction implements Function<Collection<Collection<Writable>>,Writable> {

    private final int column;

    @Override
    public Writable call(Collection<Collection<Writable>> writables) throws Exception {
        return new SelectColumnFunction(writables);
    }
}
