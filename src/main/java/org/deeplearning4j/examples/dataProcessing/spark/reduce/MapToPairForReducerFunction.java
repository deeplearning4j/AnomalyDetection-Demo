package org.deeplearning4j.examples.dataProcessing.spark.reduce;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.PairFunction;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import scala.Tuple2;

import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 20/03/2016.
 */
@AllArgsConstructor
public class MapToPairForReducerFunction implements PairFunction<Collection<Writable>,String,Collection<Writable>> {

    private final IReducer reducer;

    @Override
    public Tuple2<String, Collection<Writable>> call(Collection<Writable> writables) throws Exception {
        List<String> keyColumns = reducer.getKeyColumns();
        Schema schema = reducer.getInputSchema();
        String key;
        List<Writable> list = ((List<Writable>)writables);
        if(keyColumns.size() == 1) key = list.get(schema.getIndexOfColumn(keyColumns.get(0))).toString();
        else {
            StringBuilder sb = new StringBuilder();
            for( int i=0; i<keyColumns.size(); i++ ){
                if(i > 0) sb.append("_");
                sb.append(list.get(schema.getIndexOfColumn(keyColumns.get(i))).toString());
            }
            key = sb.toString();
        }

        return new Tuple2<>(key,writables);
    }
}
