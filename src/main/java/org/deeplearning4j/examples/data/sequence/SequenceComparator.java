package org.deeplearning4j.examples.data.sequence;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.schema.SequenceSchema;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

/**
 * Compare the time steps of a sequence
 * Created by Alex on 11/03/2016.
 */
public interface SequenceComparator extends Comparator<Collection<Writable>>, Serializable {

    void setSchema(SequenceSchema sequenceSchema);

}
