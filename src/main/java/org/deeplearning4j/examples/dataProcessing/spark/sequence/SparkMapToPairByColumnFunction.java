package org.deeplearning4j.examples.dataProcessing.spark.sequence;

import lombok.AllArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.function.PairFunction;
import org.canova.api.writable.Writable;
import scala.Tuple2;

import java.util.Collection;

/**
 * Created by Alex on 11/03/2016.
 */
@AllArgsConstructor
public class SparkMapToPairByColumnFunction implements PairFunction<Collection<Writable>,Writable,Collection<Writable>> {

    private final int keyColumnIdx;

    @Override
    public Tuple2<Writable, Collection<Writable>> call(Collection<Writable> writables) throws Exception {
        return new Tuple2<>((Writable)CollectionUtils.get(writables,keyColumnIdx),writables);
    }
}
