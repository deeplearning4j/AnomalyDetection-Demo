package org.deeplearning4j.examples.dataProcessing.api.sequence;

import org.canova.api.writable.Writable;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 16/03/2016.
 */
public interface SequenceSplit extends Serializable {

    List<Collection<Collection<Writable>>> split(Collection<Collection<Writable>> sequence);

}
