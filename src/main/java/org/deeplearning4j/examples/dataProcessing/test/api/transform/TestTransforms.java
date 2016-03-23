package org.deeplearning4j.examples.dataProcessing.test.api.transform;

import junit.framework.Assert;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.LongWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.MathOp;
import org.deeplearning4j.examples.dataProcessing.api.Transform;
import org.deeplearning4j.examples.dataProcessing.api.metadata.CategoricalMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.DoubleMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.IntegerMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.LongMetaData;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.CategoricalToOneHotTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.DuplicateColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.RemoveColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.RenameColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.integer.IntegerMathOpTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.longtransform.LongMathOpTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.real.*;
import org.deeplearning4j.examples.dataProcessing.api.transform.string.*;
import org.deeplearning4j.examples.dataProcessing.api.transform.time.DeriveColumnsFromTimeTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.time.StringToTimeTransform;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Created by Alex on 21/03/2016.
 */
public class TestTransforms {

    private static Schema getSchema(ColumnType type, String... colNames) {

        Schema.Builder schema = new Schema.Builder();

        switch (type) {
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
    public void testCategoricalToInteger() {
        Schema schema = getSchema(ColumnType.Categorical, "zero", "one", "two");

        Transform transform = new CategoricalToIntegerTransform("column");
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);


        assertEquals(ColumnType.Integer, out.getMetaData(0).getColumnType());
        IntegerMetaData meta = (IntegerMetaData) out.getMetaData(0);
        assertNotNull(meta.getMinAllowedValue());
        assertEquals(0, (int) meta.getMinAllowedValue());

        assertNotNull(meta.getMaxAllowedValue());
        assertEquals(2, (int) meta.getMaxAllowedValue());

        assertEquals(0, transform.map(Collections.singletonList((Writable) new Text("zero"))).get(0).toInt());
        assertEquals(1, transform.map(Collections.singletonList((Writable) new Text("one"))).get(0).toInt());
        assertEquals(2, transform.map(Collections.singletonList((Writable) new Text("two"))).get(0).toInt());
    }

    @Test
    public void testCategoricalToOneHotTransform() {
        Schema schema = getSchema(ColumnType.Categorical, "zero", "one", "two");

        Transform transform = new CategoricalToOneHotTransform("column");
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(3, out.getColumnMetaData().size());
        for (int i = 0; i < 3; i++) {
            assertEquals(ColumnType.Integer, out.getMetaData(i).getColumnType());
            IntegerMetaData meta = (IntegerMetaData) out.getMetaData(i);
            assertNotNull(meta.getMinAllowedValue());
            assertEquals(0, (int) meta.getMinAllowedValue());

            assertNotNull(meta.getMaxAllowedValue());
            assertEquals(1, (int) meta.getMaxAllowedValue());
        }

        assertEquals(Arrays.asList(new IntWritable(1), new IntWritable(0), new IntWritable(0)),
                transform.map(Collections.singletonList((Writable) new Text("zero"))));
        assertEquals(Arrays.asList(new IntWritable(0), new IntWritable(1), new IntWritable(0)),
                transform.map(Collections.singletonList((Writable) new Text("one"))));
        assertEquals(Arrays.asList(new IntWritable(0), new IntWritable(0), new IntWritable(1)),
                transform.map(Collections.singletonList((Writable) new Text("two"))));
    }

    @Test
    public void testIntegerToCategoricalTransform() {
        Schema schema = getSchema(ColumnType.Integer);

        Transform transform = new IntegerToCategoricalTransform("column", Arrays.asList("zero", "one", "two"));
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Categorical, out.getMetaData(0).getColumnType());
        CategoricalMetaData meta = (CategoricalMetaData) out.getMetaData(0);
        assertEquals(Arrays.asList("zero", "one", "two"), meta.getStateNames());

        assertEquals(Collections.singletonList((Writable) new Text("zero")),
                transform.map(Collections.singletonList((Writable) new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable) new Text("one")),
                transform.map(Collections.singletonList((Writable) new IntWritable(1))));
        assertEquals(Collections.singletonList((Writable) new Text("two")),
                transform.map(Collections.singletonList((Writable) new IntWritable(2))));
    }

    @Test
    public void testStringToCategoricalTransform() {
        Schema schema = getSchema(ColumnType.String);

        Transform transform = new StringToCategoricalTransform("column", Arrays.asList("zero", "one", "two"));
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Categorical, out.getMetaData(0).getColumnType());
        CategoricalMetaData meta = (CategoricalMetaData) out.getMetaData(0);
        assertEquals(Arrays.asList("zero", "one", "two"), meta.getStateNames());

        assertEquals(Collections.singletonList((Writable) new Text("zero")),
                transform.map(Collections.singletonList((Writable) new Text("zero"))));
        assertEquals(Collections.singletonList((Writable) new Text("one")),
                transform.map(Collections.singletonList((Writable) new Text("one"))));
        assertEquals(Collections.singletonList((Writable) new Text("two")),
                transform.map(Collections.singletonList((Writable) new Text("two"))));
    }

    @Test
    public void testRemoveColumnsTransform() {
        Schema schema = new Schema.Builder()
                .addColumnDouble("first")
                .addColumnString("second")
                .addColumnInteger("third")
                .addColumnLong("fourth")
                .build();

        Transform transform = new RemoveColumnsTransform("first", "fourth");
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);

        assertEquals(2, out.getColumnMetaData().size());
        assertEquals(ColumnType.String, out.getMetaData(0).getColumnType());
        assertEquals(ColumnType.Integer, out.getMetaData(1).getColumnType());

        assertEquals(Arrays.asList(new Text("one"), new IntWritable(1)),
                transform.map(Arrays.asList((Writable) new DoubleWritable(1.0), new Text("one"), new IntWritable(1), new LongWritable(1L))));
    }

    @Test
    public void testReplaceEmptyIntegerWithValueTransform() {
        Schema schema = getSchema(ColumnType.Integer);

        Transform transform = new ReplaceEmptyIntegerWithValueTransform("column", 1000);
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Integer, out.getMetaData(0).getColumnType());

        assertEquals(Collections.singletonList((Writable) new IntWritable(0)),
                transform.map(Collections.singletonList((Writable) new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable) new IntWritable(1)),
                transform.map(Collections.singletonList((Writable) new IntWritable(1))));
        assertEquals(Collections.singletonList((Writable) new IntWritable(1000)),
                transform.map(Collections.singletonList((Writable) new Text(""))));
    }

    @Test
    public void testReplaceInvalidWithIntegerTransform() {
        Schema schema = getSchema(ColumnType.Integer);

        Transform transform = new ReplaceInvalidWithIntegerTransform("column", 1000);
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Integer, out.getMetaData(0).getColumnType());

        assertEquals(Collections.singletonList((Writable) new IntWritable(0)),
                transform.map(Collections.singletonList((Writable) new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable) new IntWritable(1)),
                transform.map(Collections.singletonList((Writable) new IntWritable(1))));
        assertEquals(Collections.singletonList((Writable) new IntWritable(1000)),
                transform.map(Collections.singletonList((Writable) new Text(""))));
    }

    @Test
    public void testLog2Normalizer() {
        Schema schema = getSchema(ColumnType.Double);

        double mu = 2.0;
        double min = 1.0;
        double scale = 0.5;

        Transform transform = new Log2Normalizer("column", mu, min, scale);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Double, out.getMetaData(0).getColumnType());
        DoubleMetaData meta = (DoubleMetaData) out.getMetaData(0);
        assertNotNull(meta.getMin());
        assertEquals(0, meta.getMin(), 1e-6);
        assertNull(meta.getMax());

        double loge2 = Math.log(2);
        assertEquals(0.0, transform.map(Collections.singletonList((Writable) new DoubleWritable(min))).get(0).toDouble(), 1e-6);
        double d = scale * Math.log((10 - min) / (mu - min) + 1) / loge2;
        assertEquals(d, transform.map(Collections.singletonList((Writable) new DoubleWritable(10))).get(0).toDouble(), 1e-6);
        d = scale * Math.log((3 - min) / (mu - min) + 1) / loge2;
        assertEquals(d, transform.map(Collections.singletonList((Writable) new DoubleWritable(3))).get(0).toDouble(), 1e-6);
    }

    @Test
    public void testDoubleMinMaxNormalizerTransform() {
        Schema schema = getSchema(ColumnType.Double);

        Transform transform = new MinMaxNormalizer("column", 0, 100);
        Transform transform2 = new MinMaxNormalizer("column", 0, 100, -1, 1);
        transform.setInputSchema(schema);
        transform2.setInputSchema(schema);

        Schema out = transform.transform(schema);
        Schema out2 = transform2.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Double, out.getMetaData(0).getColumnType());
        DoubleMetaData meta = (DoubleMetaData) out.getMetaData(0);
        DoubleMetaData meta2 = (DoubleMetaData) out2.getMetaData(0);
        assertEquals(0, meta.getMin(), 1e-6);
        assertEquals(1, meta.getMax(), 1e-6);
        assertEquals(-1, meta2.getMin(), 1e-6);
        assertEquals(1, meta2.getMax(), 1e-6);


        assertEquals(0.0, transform.map(Collections.singletonList((Writable) new DoubleWritable(0))).get(0).toDouble(), 1e-6);
        assertEquals(1.0, transform.map(Collections.singletonList((Writable) new DoubleWritable(100))).get(0).toDouble(), 1e-6);
        assertEquals(0.5, transform.map(Collections.singletonList((Writable) new DoubleWritable(50))).get(0).toDouble(), 1e-6);

        assertEquals(-1.0, transform2.map(Collections.singletonList((Writable) new DoubleWritable(0))).get(0).toDouble(), 1e-6);
        assertEquals(1.0, transform2.map(Collections.singletonList((Writable) new DoubleWritable(100))).get(0).toDouble(), 1e-6);
        assertEquals(0.0, transform2.map(Collections.singletonList((Writable) new DoubleWritable(50))).get(0).toDouble(), 1e-6);
    }

    @Test
    public void testStandardizeNormalizer() {
        Schema schema = getSchema(ColumnType.Double);

        double mu = 1.0;
        double sigma = 2.0;

        Transform transform = new StandardizeNormalizer("column", mu, sigma);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Double, out.getMetaData(0).getColumnType());
        DoubleMetaData meta = (DoubleMetaData) out.getMetaData(0);
        assertNull(meta.getMin());
        assertNull(meta.getMax());


        assertEquals(0.0, transform.map(Collections.singletonList((Writable) new DoubleWritable(mu))).get(0).toDouble(), 1e-6);
        double d = (10 - mu) / sigma;
        assertEquals(d, transform.map(Collections.singletonList((Writable) new DoubleWritable(10))).get(0).toDouble(), 1e-6);
        d = (-2 - mu) / sigma;
        assertEquals(d, transform.map(Collections.singletonList((Writable) new DoubleWritable(-2))).get(0).toDouble(), 1e-6);
    }

    @Test
    public void testSubtractMeanNormalizer() {
        Schema schema = getSchema(ColumnType.Double);

        double mu = 1.0;

        Transform transform = new SubtractMeanNormalizer("column", mu);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Double, out.getMetaData(0).getColumnType());
        DoubleMetaData meta = (DoubleMetaData) out.getMetaData(0);
        assertNull(meta.getMin());
        assertNull(meta.getMax());


        assertEquals(0.0, transform.map(Collections.singletonList((Writable) new DoubleWritable(mu))).get(0).toDouble(), 1e-6);
        assertEquals(10 - mu, transform.map(Collections.singletonList((Writable) new DoubleWritable(10))).get(0).toDouble(), 1e-6);
    }

    @Test
    public void testMapAllStringsExceptListTransform() {
        Schema schema = getSchema(ColumnType.String);

        Transform transform = new MapAllStringsExceptListTransform("column", "replacement", Arrays.asList("one", "two", "three"));
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.String, out.getMetaData(0).getColumnType());

        assertEquals(Collections.singletonList((Writable) new Text("one")),
                transform.map(Collections.singletonList((Writable) new Text("one"))));
        assertEquals(Collections.singletonList((Writable) new Text("two")),
                transform.map(Collections.singletonList((Writable) new Text("two"))));
        assertEquals(Collections.singletonList((Writable) new Text("replacement")),
                transform.map(Collections.singletonList((Writable) new Text("this should be replaced"))));
    }

    @Test
    public void testRemoveWhitespaceTransform() {
        Schema schema = getSchema(ColumnType.String);

        Transform transform = new RemoveWhiteSpaceTransform("column");
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.String, out.getMetaData(0).getColumnType());

        assertEquals(Collections.singletonList((Writable) new Text("one")),
                transform.map(Collections.singletonList((Writable) new Text("one "))));
        assertEquals(Collections.singletonList((Writable) new Text("two")),
                transform.map(Collections.singletonList((Writable) new Text("two\t"))));
        assertEquals(Collections.singletonList((Writable) new Text("three")),
                transform.map(Collections.singletonList((Writable) new Text("three\n"))));
        assertEquals(Collections.singletonList((Writable) new Text("one")),
                transform.map(Collections.singletonList((Writable) new Text(" o n e\t"))));
    }

    @Test
    public void testReplaceEmptyStringTransform() {
        Schema schema = getSchema(ColumnType.String);

        Transform transform = new ReplaceEmptyStringTransform("column", "newvalue");
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.String, out.getMetaData(0).getColumnType());

        assertEquals(Collections.singletonList((Writable) new Text("one")),
                transform.map(Collections.singletonList((Writable) new Text("one"))));
        assertEquals(Collections.singletonList((Writable) new Text("newvalue")),
                transform.map(Collections.singletonList((Writable) new Text(""))));
        assertEquals(Collections.singletonList((Writable) new Text("three")),
                transform.map(Collections.singletonList((Writable) new Text("three"))));
    }

    @Test
    public void testStringListToCategoricalSetTransform() {
        //Idea: String list to a set of categories... "a,c" for categories {a,b,c} -> "true","false","true"

        Schema schema = getSchema(ColumnType.String);

        Transform transform = new StringListToCategoricalSetTransform("column", Arrays.asList("a", "b", "c"), Arrays.asList("a", "b", "c"), ",");
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);
        assertEquals(3, out.getColumnMetaData().size());
        for (int i = 0; i < 3; i++) {
            assertEquals(ColumnType.Categorical, out.getType(i));
            CategoricalMetaData meta = (CategoricalMetaData) out.getMetaData(i);
            assertEquals(Arrays.asList("true", "false"), meta.getStateNames());
        }

        assertEquals(Arrays.asList(new Text("false"), new Text("false"), new Text("false")), transform.map(Collections.singletonList((Writable) new Text(""))));
        assertEquals(Arrays.asList(new Text("true"), new Text("false"), new Text("false")), transform.map(Collections.singletonList((Writable) new Text("a"))));
        assertEquals(Arrays.asList(new Text("false"), new Text("true"), new Text("false")), transform.map(Collections.singletonList((Writable) new Text("b"))));
        assertEquals(Arrays.asList(new Text("false"), new Text("false"), new Text("true")), transform.map(Collections.singletonList((Writable) new Text("c"))));
        assertEquals(Arrays.asList(new Text("true"), new Text("false"), new Text("true")), transform.map(Collections.singletonList((Writable) new Text("a,c"))));
        assertEquals(Arrays.asList(new Text("true"), new Text("true"), new Text("true")), transform.map(Collections.singletonList((Writable) new Text("a,b,c"))));
    }

    @Test
    public void testStringMapTransform() {
        Schema schema = getSchema(ColumnType.String);

        Map<String, String> map = new HashMap<>();
        map.put("one", "ONE");
        map.put("two", "TWO");
        Transform transform = new StringMapTransform("column", map);
        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.String, out.getMetaData(0).getColumnType());

        assertEquals(Collections.singletonList((Writable) new Text("ONE")),
                transform.map(Collections.singletonList((Writable) new Text("one"))));
        assertEquals(Collections.singletonList((Writable) new Text("TWO")),
                transform.map(Collections.singletonList((Writable) new Text("two"))));
        assertEquals(Collections.singletonList((Writable) new Text("three")),
                transform.map(Collections.singletonList((Writable) new Text("three"))));
    }


    @Test
    public void testStringToTimeTransform() {
        Schema schema = getSchema(ColumnType.String);

        //http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
        Transform transform = new StringToTimeTransform("column", "YYYY-MM-dd HH:mm:ss", DateTimeZone.forID("UTC"));
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);

        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Time, out.getMetaData(0).getColumnType());

        String in1 = "2016-01-01 12:30:45";
        long out1 = 1451651445000L;

        String in2 = "2015-06-30 23:59:59";
        long out2 = 1435708799000L;

        assertEquals(Collections.singletonList((Writable) new LongWritable(out1)),
                transform.map(Collections.singletonList((Writable) new Text(in1))));
        assertEquals(Collections.singletonList((Writable) new LongWritable(out2)),
                transform.map(Collections.singletonList((Writable) new Text(in2))));
    }

    @Test
    public void testDeriveColumnsFromTimeTransform() {
        Schema schema = new Schema.Builder()
                .addColumnTime("column", DateTimeZone.forID("UTC"))
                .build();

        Transform transform = new DeriveColumnsFromTimeTransform.Builder("column")
                .addIntegerDerivedColumn("hour", DateTimeFieldType.hourOfDay())
                .addIntegerDerivedColumn("day", DateTimeFieldType.dayOfMonth())
                .addIntegerDerivedColumn("second", DateTimeFieldType.secondOfMinute())
                .addStringDerivedColumn("humanReadable", new DateTimeFormatterBuilder()
                        .appendYear(4, 4)
                        .appendLiteral("-")
                        .appendMonthOfYear(2)
                        .appendLiteral("-")
                        .appendDayOfMonth(2)
                        .appendLiteral(" ")
                        .appendHourOfDay(2)
                        .appendLiteral(":")
                        .appendMinuteOfHour(2)
                        .appendLiteral(":")
                        .appendSecondOfMinute(2)
                        .toFormatter()
                        .withZone(DateTimeZone.forID("UTC")))
                .build();

        transform.setInputSchema(schema);
        Schema out = transform.transform(schema);

        assertEquals(5, out.getColumnMetaData().size());
        assertEquals(ColumnType.Time, out.getMetaData(0).getColumnType());
        assertEquals(ColumnType.Integer, out.getMetaData(1).getColumnType());
        assertEquals(ColumnType.Integer, out.getMetaData(2).getColumnType());
        assertEquals(ColumnType.Integer, out.getMetaData(3).getColumnType());
        assertEquals(ColumnType.String, out.getMetaData(4).getColumnType());

        assertEquals("column", out.getName(0));
        assertEquals("hour", out.getName(1));
        assertEquals("day", out.getName(2));
        assertEquals("second", out.getName(3));
        assertEquals("humanReadable", out.getName(4));

        long in1 = 1451651445000L;      //"2016-01-01 12:30:45" GMT

        List<Writable> out1 = new ArrayList<>();
        out1.add(new LongWritable(in1));
        out1.add(new IntWritable(12));  //hour
        out1.add(new IntWritable(1));   //day
        out1.add(new IntWritable(45));  //second
        out1.add(new Text("2016-01-01 12:30:45"));

        long in2 = 1435708799000L;      //"2015-06-30 23:59:59" GMT
        List<Writable> out2 = new ArrayList<>();
        out2.add(new LongWritable(in2));
        out2.add(new IntWritable(23));  //hour
        out2.add(new IntWritable(30));   //day
        out2.add(new IntWritable(59));  //second
        out2.add(new Text("2015-06-30 23:59:59"));

        assertEquals(out1, transform.map(Collections.singletonList((Writable) new LongWritable(in1))));
        assertEquals(out2, transform.map(Collections.singletonList((Writable) new LongWritable(in2))));
    }


    @Test
    public void testDuplicateColumnsTransform() {

        Schema schema = new Schema.Builder()
                .addColumnString("stringCol")
                .addColumnInteger("intCol")
                .addColumnLong("longCol")
                .build();

        List<String> toDup = Arrays.asList("intCol", "longCol");
        List<String> newNames = Arrays.asList("dup_intCol", "dup_longCol");

        Transform transform = new DuplicateColumnsTransform(toDup, newNames);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);
        assertEquals(5, out.getColumnMetaData().size());

        List<String> expOutNames = Arrays.asList("stringCol", "intCol", "dup_intCol", "longCol", "dup_longCol");
        List<ColumnType> expOutTypes = Arrays.asList(ColumnType.String, ColumnType.Integer, ColumnType.Integer, ColumnType.Long, ColumnType.Long);
        for (int i = 0; i < 5; i++) {
            assertEquals(expOutNames.get(i), out.getName(i));
            assertEquals(expOutTypes.get(i), out.getType(i));
        }

        List<Writable> inList = Arrays.asList((Writable) new Text("one"), new IntWritable(2), new LongWritable(3L));
        List<Writable> outList = Arrays.asList((Writable) new Text("one"), new IntWritable(2), new IntWritable(2), new LongWritable(3L), new LongWritable(3L));

        assertEquals(outList, transform.map(inList));
    }

    @Test
    public void testIntegerMathOpTransform(){
        Schema schema = new Schema.Builder()
                .addColumnInteger("column",-1,1)
                .build();

        Transform transform = new IntegerMathOpTransform("column", MathOp.Multiply,5);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);
        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Integer, out.getType(0));
        IntegerMetaData meta = (IntegerMetaData)out.getMetaData(0);
        assertEquals(-5, (int)meta.getMinAllowedValue());
        assertEquals(5, (int)meta.getMaxAllowedValue());

        assertEquals(Collections.singletonList((Writable)new IntWritable(-5)), transform.map(Collections.singletonList((Writable)new IntWritable(-1))));
        assertEquals(Collections.singletonList((Writable)new IntWritable(0)), transform.map(Collections.singletonList((Writable)new IntWritable(0))));
        assertEquals(Collections.singletonList((Writable)new IntWritable(5)), transform.map(Collections.singletonList((Writable)new IntWritable(1))));
    }

    @Test
    public void testLongMathOpTransform(){
        Schema schema = new Schema.Builder()
                .addColumnLong("column",-1L,1L)
                .build();

        Transform transform = new LongMathOpTransform("column", MathOp.Multiply,5);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);
        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Long, out.getType(0));
        LongMetaData meta = (LongMetaData) out.getMetaData(0);
        assertEquals(-5, (long)meta.getMin());
        assertEquals(5, (long)meta.getMax());

        assertEquals(Collections.singletonList((Writable)new LongWritable(-5)), transform.map(Collections.singletonList((Writable)new LongWritable(-1))));
        assertEquals(Collections.singletonList((Writable)new LongWritable(0)), transform.map(Collections.singletonList((Writable)new LongWritable(0))));
        assertEquals(Collections.singletonList((Writable)new LongWritable(5)), transform.map(Collections.singletonList((Writable)new LongWritable(1))));
    }

    @Test
    public void testDoubleMathOpTransform(){
        Schema schema = new Schema.Builder()
                .addColumnDouble("column",-1.0,1.0)
                .build();

        Transform transform = new DoubleMathOpTransform("column", MathOp.Multiply,5.0);
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);
        assertEquals(1, out.getColumnMetaData().size());
        assertEquals(ColumnType.Double, out.getType(0));
        DoubleMetaData meta = (DoubleMetaData)out.getMetaData(0);
        assertEquals(-5.0, meta.getMin(), 1e-6);
        assertEquals(5.0, meta.getMax(), 1e-6);

        assertEquals(Collections.singletonList((Writable)new DoubleWritable(-5)), transform.map(Collections.singletonList((Writable)new DoubleWritable(-1))));
        assertEquals(Collections.singletonList((Writable)new DoubleWritable(0)), transform.map(Collections.singletonList((Writable)new DoubleWritable(0))));
        assertEquals(Collections.singletonList((Writable)new DoubleWritable(5)), transform.map(Collections.singletonList((Writable)new DoubleWritable(1))));
    }

    @Test
    public void testRenameColumnsTransform(){

        Schema schema = new Schema.Builder()
                .addColumnDouble("col1")
                .addColumnString("col2")
                .addColumnInteger("col3")
                .build();

        Transform transform = new RenameColumnsTransform(Arrays.asList("col1","col3"), Arrays.asList("column1","column3"));
        transform.setInputSchema(schema);

        Schema out = transform.transform(schema);

        assertEquals(3, out.getColumnMetaData().size());
        assertEquals(ColumnType.Double, out.getMetaData(0).getColumnType());
        assertEquals(ColumnType.String, out.getMetaData(1).getColumnType());
        assertEquals(ColumnType.Integer, out.getMetaData(2).getColumnType());

        assertEquals("column1", out.getName(0));
        assertEquals("col2", out.getName(1));
        assertEquals("column3", out.getName(2));
    }

}
