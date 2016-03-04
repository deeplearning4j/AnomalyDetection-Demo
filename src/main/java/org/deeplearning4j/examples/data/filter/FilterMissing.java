package org.deeplearning4j.examples.data.filter;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Filter;

import java.util.Collection;

/**
 * Created by Alex on 4/03/2016.
 */
public class FilterMissing implements Filter {


    @Override
    public boolean removeExample(Collection<Writable> writables) {
        throw new UnsupportedOperationException("Not yet implemneted");
    }
}
