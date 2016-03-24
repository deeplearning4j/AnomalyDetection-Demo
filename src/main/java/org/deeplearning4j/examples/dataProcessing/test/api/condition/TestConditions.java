package org.deeplearning4j.examples.dataProcessing.test.api.condition;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.condition.Condition;
import org.deeplearning4j.examples.dataProcessing.api.condition.ConditionOp;
import org.deeplearning4j.examples.dataProcessing.api.condition.SequenceConditionMode;
import org.deeplearning4j.examples.dataProcessing.api.condition.column.IntegerColumnCondition;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.test.api.transform.TestTransforms;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

}
