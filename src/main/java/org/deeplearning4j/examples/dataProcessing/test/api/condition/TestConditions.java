package org.deeplearning4j.examples.dataProcessing.test.api.condition;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.LongWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.condition.Condition;
import org.deeplearning4j.examples.dataProcessing.api.condition.ConditionOp;
import org.deeplearning4j.examples.dataProcessing.api.condition.SequenceConditionMode;
import org.deeplearning4j.examples.dataProcessing.api.condition.column.*;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.test.api.transform.TestTransforms;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by Alex on 24/03/2016.
 */
public class TestConditions {

    @Test
    public void testIntegerCondition(){
        Schema schema = TestTransforms.getSchema(ColumnType.Integer);

        Condition condition = new IntegerColumnCondition("column", SequenceConditionMode.Or, ConditionOp.LessThan, 0);
        condition.setInputSchema(schema);

        assertTrue(condition.condition(Collections.singletonList((Writable)new IntWritable(-1))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new IntWritable(-2))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new IntWritable(0))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new IntWritable(1))));

        Set<Integer> set = new HashSet<>();
        set.add(0);
        set.add(3);
        condition = new IntegerColumnCondition("column", SequenceConditionMode.Or, ConditionOp.InSet, set);
        condition.setInputSchema(schema);
        assertTrue(condition.condition(Collections.singletonList((Writable)new IntWritable(0))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new IntWritable(3))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new IntWritable(1))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new IntWritable(2))));
    }

    @Test
    public void testLongCondition(){
        Schema schema = TestTransforms.getSchema(ColumnType.Long);

        Condition condition = new LongColumnCondition("column", SequenceConditionMode.Or, ConditionOp.NotEqual, 5L);
        condition.setInputSchema(schema);

        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(0))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(1))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new LongWritable(5))));

        Set<Long> set = new HashSet<>();
        set.add(0L);
        set.add(3L);
        condition = new LongColumnCondition("column", SequenceConditionMode.Or, ConditionOp.NotInSet, set);
        condition.setInputSchema(schema);
        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(5))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(10))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new LongWritable(0))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new LongWritable(3))));
    }

    @Test
    public void testDoubleCondition(){
        Schema schema = TestTransforms.getSchema(ColumnType.Double);

        Condition condition = new DoubleColumnCondition("column", SequenceConditionMode.Or, ConditionOp.GreaterOrEqual, 0);
        condition.setInputSchema(schema);

        assertTrue(condition.condition(Collections.singletonList((Writable)new DoubleWritable(0.0))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new DoubleWritable(0.5))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new DoubleWritable(-0.5))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new DoubleWritable(-1))));

        Set<Double> set = new HashSet<>();
        set.add(0.0);
        set.add(3.0);
        condition = new DoubleColumnCondition("column", SequenceConditionMode.Or, ConditionOp.InSet, set);
        condition.setInputSchema(schema);
        assertTrue(condition.condition(Collections.singletonList((Writable)new DoubleWritable(0.0))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new DoubleWritable(3.0))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new DoubleWritable(1.0))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new DoubleWritable(2.0))));
    }

    @Test
    public void testStringCondition(){
        Schema schema = TestTransforms.getSchema(ColumnType.Integer);

        Condition condition = new StringColumnCondition("column", SequenceConditionMode.Or, ConditionOp.Equal, "value");
        condition.setInputSchema(schema);

        assertTrue(condition.condition(Collections.singletonList((Writable)new Text("value"))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new Text("not_value"))));

        Set<String> set = new HashSet<>();
        set.add("in set");
        set.add("also in set");
        condition = new StringColumnCondition("column", SequenceConditionMode.Or, ConditionOp.InSet, set);
        condition.setInputSchema(schema);
        assertTrue(condition.condition(Collections.singletonList((Writable)new Text("in set"))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new Text("also in set"))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new Text("not in the set"))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new Text(":)"))));
    }

    @Test
    public void testCategoricalCondition(){
        Schema schema = new Schema.Builder()
                .addColumnCategorical("column", "alpha", "beta", "gamma")
                .build();

        Condition condition = new CategoricalColumnCondition("column", SequenceConditionMode.Or, ConditionOp.Equal, "alpha");
        condition.setInputSchema(schema);

        assertTrue(condition.condition(Collections.singletonList((Writable)new Text("alpha"))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new Text("beta"))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new Text("gamma"))));

        Set<String> set = new HashSet<>();
        set.add("alpha");
        set.add("beta");
        condition = new StringColumnCondition("column", SequenceConditionMode.Or, ConditionOp.InSet, set);
        condition.setInputSchema(schema);
        assertTrue(condition.condition(Collections.singletonList((Writable)new Text("alpha"))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new Text("beta"))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new Text("gamma"))));
    }

    @Test
    public void testTimeCondition(){
        Schema schema = TestTransforms.getSchema(ColumnType.Integer);

        //1451606400000 = 01/01/2016 00:00:00 GMT
        Condition condition = new TimeColumnCondition("column", SequenceConditionMode.Or, ConditionOp.LessOrEqual, 1451606400000L);
        condition.setInputSchema(schema);

        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(1451606400000L))));
        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(1451606400000L - 1L))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new LongWritable(1451606400000L + 1L))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new LongWritable(1451606400000L + 1000L))));

        Set<Long> set = new HashSet<>();
        set.add(1451606400000L);
        condition = new TimeColumnCondition("column", SequenceConditionMode.Or, ConditionOp.InSet, set);
        condition.setInputSchema(schema);
        assertTrue(condition.condition(Collections.singletonList((Writable)new LongWritable(1451606400000L))));
        assertFalse(condition.condition(Collections.singletonList((Writable)new LongWritable(1451606400000L + 1L))));
    }

}
