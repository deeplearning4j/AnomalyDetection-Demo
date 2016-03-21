package org.deeplearning4j.examples.dataProcessing.test.api.filter;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.filter.Filter;
import org.deeplearning4j.examples.dataProcessing.api.filter.FilterInvalidValues;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Alex on 21/03/2016.
 */
public class TestFilterInvalidValues {

    @Test
    public void testFilterInvalidValues(){

        List<List<Writable>> list = new ArrayList<>();
        list.add(Collections.singletonList((Writable)new IntWritable(-1)));
        list.add(Collections.singletonList((Writable)new IntWritable(0)));
        list.add(Collections.singletonList((Writable)new IntWritable(2)));

        Schema schema = new Schema.Builder()
                .addColumnInteger("intCol",0,10)        //Only values in the range 0 to 10 are ok
                .addColumnDouble("doubleCol",-100.0,100.0)  //-100 to 100 only; no NaN or infinite
                .build();

        Filter filter = new FilterInvalidValues("intCol","doubleCol");
        filter.setSchema(schema);

        //Test valid examples:
        assertFalse(filter.removeExample(Arrays.asList((Writable)new IntWritable(0),new DoubleWritable(0))));
        assertFalse(filter.removeExample(Arrays.asList((Writable)new IntWritable(10),new DoubleWritable(0))));
        assertFalse(filter.removeExample(Arrays.asList((Writable)new IntWritable(0),new DoubleWritable(-100))));
        assertFalse(filter.removeExample(Arrays.asList((Writable)new IntWritable(0),new DoubleWritable(100))));

        //Test invalid:
        assertTrue(filter.removeExample(Arrays.asList((Writable)new IntWritable(-1),new DoubleWritable(0))));
        assertTrue(filter.removeExample(Arrays.asList((Writable)new IntWritable(11),new DoubleWritable(0))));
        assertTrue(filter.removeExample(Arrays.asList((Writable)new IntWritable(0),new DoubleWritable(-101))));
        assertTrue(filter.removeExample(Arrays.asList((Writable)new IntWritable(0),new DoubleWritable(101))));

    }

}
