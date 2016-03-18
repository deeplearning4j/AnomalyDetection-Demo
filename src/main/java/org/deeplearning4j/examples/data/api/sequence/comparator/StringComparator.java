package org.deeplearning4j.examples.data.api.sequence.comparator;

import org.canova.api.writable.Writable;

/**
 * Created by Alex on 11/03/2016.
 */
public class StringComparator extends BaseColumnComparator {

    public StringComparator(String columnName) {
        super(columnName);
    }

    @Override
    protected int compare(Writable w1, Writable w2) {
        return w1.toString().compareTo(w2.toString());
    }
}
