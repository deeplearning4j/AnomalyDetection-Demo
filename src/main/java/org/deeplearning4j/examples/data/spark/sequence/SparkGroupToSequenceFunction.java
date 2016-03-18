package org.deeplearning4j.examples.data.spark.sequence;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.sequence.SequenceComparator;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Alex on 11/03/2016.
 */
@AllArgsConstructor
public class SparkGroupToSequenceFunction implements Function<Tuple2<Writable,Iterable<Collection<Writable>>>,Collection<Collection<Writable>>> {

    private final SequenceComparator comparator;

    @Override
    public Collection<Collection<Writable>> call(Tuple2<Writable, Iterable<Collection<Writable>>> tuple) throws Exception {

        List<Collection<Writable>> list = new ArrayList<>();
        for (Collection<Writable> writables : tuple._2()) list.add(writables);

        Collections.sort(list,comparator);

        return list;
    }
}
