package org.deeplearning4j.examples.dataProcessing.spark.reduce;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.PairFunction;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import scala.Tuple2;

import java.util.List;


@AllArgsConstructor
public class MapToPairForReducerFunction implements PairFunction<List<Writable>,String,List<Writable>> {

    private final IReducer reducer;

    @Override
    public Tuple2<String, List<Writable>> call(List<Writable> writables) throws Exception {
        List<String> keyColumns = reducer.getKeyColumns();
        Schema schema = reducer.getInputSchema();
        String key;
        if(keyColumns.size() == 1) key = writables.get(schema.getIndexOfColumn(keyColumns.get(0))).toString();
        else {
            StringBuilder sb = new StringBuilder();
            for( int i=0; i<keyColumns.size(); i++ ){
                if(i > 0) sb.append("_");
                sb.append(writables.get(schema.getIndexOfColumn(keyColumns.get(i))).toString());
            }
            key = sb.toString();
        }

        return new Tuple2<>(key,writables);
    }
}
