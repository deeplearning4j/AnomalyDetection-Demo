package org.deeplearning4j.examples.dataProcessing.test.api.reduce;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ReduceOp;
import org.deeplearning4j.examples.dataProcessing.api.reduce.Reducer;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by Alex on 21/03/2016.
 */
public class TestReduce {

    @Test
    public void testReducerDouble(){

        List<List<Writable>> inputs = new ArrayList<>();
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new DoubleWritable(0)));
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new DoubleWritable(1)));
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new DoubleWritable(2)));
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new DoubleWritable(2)));

        Map<ReduceOp,Double> exp = new LinkedHashMap<>();
        exp.put(ReduceOp.Min,0.0);
        exp.put(ReduceOp.Max,2.0);
        exp.put(ReduceOp.Range,2.0);
        exp.put(ReduceOp.Sum,5.0);
        exp.put(ReduceOp.Mean,1.25);
        exp.put(ReduceOp.Stdev,0.957427108);
        exp.put(ReduceOp.Count,4.0);
        exp.put(ReduceOp.CountUnique,3.0);
        exp.put(ReduceOp.TakeFirst,0.0);
        exp.put(ReduceOp.TakeLast,2.0);

        for(ReduceOp op : exp.keySet()){

            Schema schema = new Schema.Builder()
                    .addColumnString("key")
                    .addColumnDouble("column").build();

            Reducer reducer = new Reducer.Builder(op)
                    .keyColumns("key")
                    .build();

            reducer.setInputSchema(schema);

            List<Writable> out = reducer.reduce(inputs);

            assertEquals(2,out.size());

            assertEquals(out.get(0),new Text("someKey"));

            String msg = op.toString();
            assertEquals(msg, exp.get(op), out.get(1).toDouble(), 1e-5);
        }
    }

    @Test
    public void testReducerInteger(){

        List<List<Writable>> inputs = new ArrayList<>();
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new IntWritable(0)));
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new IntWritable(1)));
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new IntWritable(2)));
        inputs.add(Arrays.asList((Writable)new Text("someKey"), new IntWritable(2)));

        Map<ReduceOp,Double> exp = new LinkedHashMap<>();
        exp.put(ReduceOp.Min,0.0);
        exp.put(ReduceOp.Max,2.0);
        exp.put(ReduceOp.Range,2.0);
        exp.put(ReduceOp.Sum,5.0);
        exp.put(ReduceOp.Mean,1.25);
        exp.put(ReduceOp.Stdev,0.957427108);
        exp.put(ReduceOp.Count,4.0);
        exp.put(ReduceOp.CountUnique,3.0);
        exp.put(ReduceOp.TakeFirst,0.0);
        exp.put(ReduceOp.TakeLast,2.0);

        for(ReduceOp op : exp.keySet()){

            Schema schema = new Schema.Builder()
                    .addColumnString("key")
                    .addColumnInteger("column").build();

            Reducer reducer = new Reducer.Builder(op)
                    .keyColumns("key")
                    .build();

            reducer.setInputSchema(schema);

            List<Writable> out = reducer.reduce(inputs);

            assertEquals(2,out.size());

            assertEquals(out.get(0),new Text("someKey"));

            String msg = op.toString();
            assertEquals(msg, exp.get(op), out.get(1).toDouble(), 1e-5);
        }
    }
}
