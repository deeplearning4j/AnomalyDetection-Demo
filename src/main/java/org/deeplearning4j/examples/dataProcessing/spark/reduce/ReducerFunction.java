package org.deeplearning4j.examples.dataProcessing.spark.reduce;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 20/03/2016.
 */
@AllArgsConstructor
public class ReducerFunction implements Function<Tuple2<String,Iterable<Collection<Writable>>>,Collection<Writable>> {

    private final IReducer reducer;

    @Override
    public Collection<Writable> call(Tuple2<String, Iterable<Collection<Writable>>> t2) throws Exception {
        List<List<Writable>> list = new ArrayList<>();
        for(Collection<Writable> c : t2._2()){
            if(c instanceof List) list.add((List<Writable>)c);
            else list.add(new ArrayList<>(c));
        }
        return reducer.reduce(list);
    }
}
