package org.deeplearning4j.examples.data.normalize;

import org.canova.api.writable.Writable;

import java.util.Collection;

/**
 * Created by Alex on 13/03/2016.
 */
public interface IDataNormalizer {

    Collection<Writable> normalize(Collection<Writable> data);

    Collection<Collection<Writable>> normalizeSequence(Collection<Collection<Writable>> data);


}
