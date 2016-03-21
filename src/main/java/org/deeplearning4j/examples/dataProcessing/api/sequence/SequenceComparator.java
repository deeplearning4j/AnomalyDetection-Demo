package org.deeplearning4j.examples.dataProcessing.api.sequence;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.SequenceSchema;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Compare the time steps of a sequence
 * Created by Alex on 11/03/2016.
 */
public interface SequenceComparator extends Comparator<List<Writable>>, Serializable {

    void setSchema(SequenceSchema sequenceSchema);

}
