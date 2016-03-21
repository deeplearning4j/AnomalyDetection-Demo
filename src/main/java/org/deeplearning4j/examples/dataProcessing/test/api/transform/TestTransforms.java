package org.deeplearning4j.examples.dataProcessing.test.api.transform;

import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.Transform;
import org.deeplearning4j.examples.dataProcessing.api.metadata.CategoricalMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.IntegerMetaData;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.CategoricalToOneHotTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by Alex on 21/03/2016.
 */
public class TestTransforms {

    private static Schema getSchema(ColumnType type, String... colNames){

        Schema.Builder schema = new Schema.Builder();

        switch (type){
            case String:
                schema.addColumnString("column");
                break;
            case Integer:
                schema.addColumnInteger("column");
                break;
            case Long:
                schema.addColumnLong("column");
                break;
            case Double:
                schema.addColumnDouble("column");
                break;
            case Categorical:
                schema.addColumnCategorical("column", colNames);
                break;
            default:
                throw new RuntimeException();
        }
        return schema.build();
    }

    @Test
    public void testCategoricalToInteger(){
        Schema schema = getSchema(ColumnType.Categorical,"zero","one","two");

        Transform transform = new CategoricalToIntegerTransform("column");
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);



        assertEquals(ColumnType.Integer,out.getMetaData(0).getColumnType());
        IntegerMetaData meta = (IntegerMetaData)out.getMetaData(0);
        assertNotNull(meta.getMinAllowedValue());
        assertEquals(0,(int)meta.getMinAllowedValue());

        assertNotNull(meta.getMaxAllowedValue());
        assertEquals(2,(int)meta.getMaxAllowedValue());

        assertEquals(0,transform.map(Collections.singletonList((Writable)new Text("zero"))).get(0).toInt());
        assertEquals(1,transform.map(Collections.singletonList((Writable)new Text("one"))).get(0).toInt());
        assertEquals(2,transform.map(Collections.singletonList((Writable)new Text("two"))).get(0).toInt());
    }

    @Test
    public void testCategoricalToOneHotTransform(){
        Schema schema = getSchema(ColumnType.Categorical,"zero","one","two");

        Transform transform = new CategoricalToOneHotTransform("column");
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(3,out.getColumnMetaData().size());
        for( int i=0; i<3; i++ ){
            assertEquals(ColumnType.Integer,out.getMetaData(i).getColumnType());
            IntegerMetaData meta = (IntegerMetaData)out.getMetaData(i);
            assertNotNull(meta.getMinAllowedValue());
            assertEquals(0,(int)meta.getMinAllowedValue());

            assertNotNull(meta.getMaxAllowedValue());
            assertEquals(1,(int)meta.getMaxAllowedValue());
        }

        assertEquals(Arrays.asList(new IntWritable(1),new IntWritable(0),new IntWritable(0)),
                transform.map(Collections.singletonList((Writable)new Text("zero"))));
        assertEquals(Arrays.asList(new IntWritable(0),new IntWritable(1),new IntWritable(0)),
                transform.map(Collections.singletonList((Writable)new Text("one"))));
        assertEquals(Arrays.asList(new IntWritable(0),new IntWritable(0),new IntWritable(1)),
                transform.map(Collections.singletonList((Writable)new Text("two"))));
    }

    @Test
    public void testIntegerToCategoricalTransform(){
        Schema schema = getSchema(ColumnType.Integer);

        Transform transform = new IntegerToCategoricalTransform("column",Arrays.asList("zero","one","two"));
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1,out.getColumnMetaData().size());
        assertEquals(ColumnType.Categorical,out.getMetaData(0).getColumnType());
        CategoricalMetaData meta = (CategoricalMetaData) out.getMetaData(0);
        assertEquals(Arrays.asList("zero","one","two"),meta.getStateNames());

        assertEquals(Collections.singletonList((Writable)new Text("zero")),
                transform.map(Collections.singletonList((Writable)new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable)new Text("one")),
                transform.map(Collections.singletonList((Writable)new IntWritable(1))));
        assertEquals(Collections.singletonList((Writable)new Text("two")),
                transform.map(Collections.singletonList((Writable)new IntWritable(2))));
    }

    @Test
    public void testStringToCategoricalTransform(){
        Schema schema = getSchema(ColumnType.String);

        Transform transform = new StringToCategoricalTransform("column",Arrays.asList("zero","one","two"));
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1,out.getColumnMetaData().size());
        assertEquals(ColumnType.Categorical,out.getMetaData(0).getColumnType());
        CategoricalMetaData meta = (CategoricalMetaData) out.getMetaData(0);
        assertEquals(Arrays.asList("zero","one","two"),meta.getStateNames());

        assertEquals(Collections.singletonList((Writable)new Text("zero")),
                transform.map(Collections.singletonList((Writable)new Text("zero"))));
        assertEquals(Collections.singletonList((Writable)new Text("one")),
                transform.map(Collections.singletonList((Writable)new Text("one"))));
        assertEquals(Collections.singletonList((Writable)new Text("two")),
                transform.map(Collections.singletonList((Writable)new Text("two"))));
    }

    @Test
    public void testRemoveColumnsTransform(){
        fail("Not implemented");
    }

    @Test
    public void testReplaceEmptyIntegerWithValueTransform(){
        Schema schema = getSchema(ColumnType.Integer);

        Transform transform = new ReplaceEmptyIntegerWithValueTransform("column",1000);
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1,out.getColumnMetaData().size());
        assertEquals(ColumnType.Integer,out.getMetaData(0).getColumnType());
        IntegerMetaData meta = (IntegerMetaData) out.getMetaData(0);

        assertEquals(Collections.singletonList((Writable)new IntWritable(0)),
                transform.map(Collections.singletonList((Writable)new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable)new IntWritable(1)),
                transform.map(Collections.singletonList((Writable)new IntWritable(1))));
        assertEquals(Collections.singletonList((Writable)new IntWritable(1000)),
                transform.map(Collections.singletonList((Writable)new Text(""))));
    }

    @Test
    public void testRemplaceInvalidWithIntegerTransform(){
        Schema schema = getSchema(ColumnType.Integer);

        Transform transform = new ReplaceInvalidWithIntegerTransform("column",1000);
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1,out.getColumnMetaData().size());
        assertEquals(ColumnType.Integer,out.getMetaData(0).getColumnType());
        IntegerMetaData meta = (IntegerMetaData) out.getMetaData(0);

        assertEquals(Collections.singletonList((Writable)new IntWritable(0)),
                transform.map(Collections.singletonList((Writable)new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable)new IntWritable(1)),
                transform.map(Collections.singletonList((Writable)new IntWritable(1))));
        assertEquals(Collections.singletonList((Writable)new IntWritable(1000)),
                transform.map(Collections.singletonList((Writable)new Text(""))));
    }

    @Test
    public void testLog2Normalizer(){
        fail("Not implemented");
    }

    @Test
    public void testDoubleMinMaxNormalizerTransform(){
        fail("Not implemented");
    }

    @Test
    public void testStandardizeNormalizer(){
        fail("Not implemented");
    }

    @Test
    public void testSubtractMeanNormalizer(){
        fail("Not implemented");
    }

    @Test
    public void testMapAllStringsExceptListTransform(){
        fail("Not implemented");
    }

    @Test
    public void testRemoveWhitespaceTransform(){
        fail("Not implemented");
    }

    @Test
    public void testReplaceEmptyStringTransform(){
        fail("Not implemented");
    }

    @Test
    public void testStringListToCategoricalSetTransform(){
        fail("Not implemented");
    }

    @Test
    public void testStringMapTransform(){
        fail("Not implemented");
    }

}
