package org.deeplearning4j.examples.data;

import org.canova.api.writable.Writable;

import java.io.Serializable;
import java.util.Collection;

/**
 * Created by Alex on 4/03/2016.
 */
public interface Transform extends Serializable {

    /** Get the output schema for this transformation, given an input schema */
    Schema transform(Schema inputSchema);

    /** Set the input schema. Should be done automatically in TransformationSequence, and is often necessary
     * to do {@link #map(Collection)}
     */
    void setInputSchema(Schema inputSchema);

    Collection<Writable> map(Collection<Writable> writables);

}
