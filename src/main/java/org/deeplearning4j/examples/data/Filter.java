package org.deeplearning4j.examples.data;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.schema.Schema;

import java.io.Serializable;
import java.util.Collection;

/**
 * Created by Alex on 4/03/2016.
 */
public interface Filter extends Serializable {

    boolean removeExample(Collection<Writable> writables);

    boolean removeSequence(Collection<Collection<Writable>> sequence);

    void setSchema(Schema schema);

}
