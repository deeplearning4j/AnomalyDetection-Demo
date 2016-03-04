package org.deeplearning4j.examples.data.analysis.sparkfunctions;

import org.apache.spark.api.java.function.PairFunction;
import org.canova.api.writable.Writable;
import scala.Tuple2;

/**
 * Created by Alex on 4/03/2016.
 */
public class CategoricalToPairFunction implements PairFunction<Writable,String,Integer> {
    @Override
    public Tuple2<String, Integer> call(Writable writable) throws Exception {
        return new Tuple2<>(writable.toString(),1);
    }
}
