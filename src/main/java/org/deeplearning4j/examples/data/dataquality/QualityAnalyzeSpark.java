package org.deeplearning4j.examples.data.dataquality;

import org.apache.spark.api.java.JavaRDD;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.SelectSequnceFunction;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.SelectColumnFunction;
import org.deeplearning4j.examples.data.dataquality.columns.*;
import org.deeplearning4j.examples.data.dataquality.spark.categorical.CategoricalQualityAddFunction;
import org.deeplearning4j.examples.data.dataquality.spark.categorical.CategoricalQualityMergeFunction;
import org.deeplearning4j.examples.data.dataquality.spark.integer.IntegerQualityAddFunction;
import org.deeplearning4j.examples.data.dataquality.spark.integer.IntegerQualityMergeFunction;
import org.deeplearning4j.examples.data.dataquality.spark.longq.LongQualityAddFunction;
import org.deeplearning4j.examples.data.dataquality.spark.longq.LongQualityMergeFunction;
import org.deeplearning4j.examples.data.dataquality.spark.real.RealQualityAddFunction;
import org.deeplearning4j.examples.data.dataquality.spark.real.RealQualityMergeFunction;
import org.deeplearning4j.examples.data.dataquality.spark.string.StringQualityAddFunction;
import org.deeplearning4j.examples.data.dataquality.spark.string.StringQualityMergeFunction;
import org.deeplearning4j.examples.data.meta.*;
import org.deeplearning4j.examples.data.schema.SequenceSchema;
import org.deeplearning4j.examples.data.spark.FilterWritablesBySchemaFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class QualityAnalyzeSpark {

    protected static List<ColumnQuality> list;

    private static void analyze(ColumnMetaData meta, JavaRDD<Writable> ithColumn){

        switch(meta.getColumnType()){
            case String:
                ithColumn.cache();
                long countUnique = ithColumn.distinct().count();

                StringQuality initialString = new StringQuality();
                StringQuality stringQuality = ithColumn.aggregate(initialString,new StringQualityAddFunction((StringMetaData)meta),new StringQualityMergeFunction());

                list.add(stringQuality.add(new StringQuality(0,0,0,0,0,0,0,0,0,countUnique)));
                break;
            case Integer:
                IntegerQuality initialInt = new IntegerQuality(0,0,0,0,0);
                IntegerQuality integerQuality = ithColumn.aggregate(initialInt,new IntegerQualityAddFunction((IntegerMetaData)meta),new IntegerQualityMergeFunction());
                list.add(integerQuality);
                break;
            case Long:
                LongQuality initialLong = new LongQuality();
                LongQuality longQuality = ithColumn.aggregate(initialLong,new LongQualityAddFunction((LongMetaData)meta),new LongQualityMergeFunction());
                list.add(longQuality);
                break;
            case Double:
                RealQuality initialReal = new RealQuality();
                RealQuality realQuality = ithColumn.aggregate(initialReal,new RealQualityAddFunction((DoubleMetaData)meta), new RealQualityMergeFunction());
                list.add(realQuality);
                break;
            case Categorical:
                CategoricalQuality initialCat = new CategoricalQuality();
                CategoricalQuality categoricalQuality = ithColumn.aggregate(initialCat,new CategoricalQualityAddFunction((CategoricalMetaData)meta),new CategoricalQualityMergeFunction());

                list.add(categoricalQuality);


                break;
            case BLOB:
                list.add(new BlobQuality());    //TODO
                break;
        }
    }

    public static List<DataQualityAnalysis> analyzeQuality(JavaRDD<Collection<Collection<Writable>>> data, Schema schema){
        int nTimeSteps = schema.numColumns();
        List<DataQualityAnalysis> timeStepDQA = new ArrayList<>(nTimeSteps);
        for( int i=0; i < nTimeSteps; i++ ) {
            JavaRDD<Collection<Writable>> ithStep = data.map(new SelectSequnceFunction(i));
            timeStepDQA.add(analyzeQuality(schema, ithStep));
        }
        return timeStepDQA;
    }

    public static DataQualityAnalysis analyzeQuality(Schema schema, JavaRDD<Collection<Writable>> data){

        data.cache();
        int nColumns = schema.numColumns();

        //This is inefficient, but it's easy to implement. Good enough for now!
        list = new ArrayList<>(nColumns);

        for( int i=0; i<nColumns; i++ ) {
            ColumnMetaData meta = schema.getMetaData(i);
            JavaRDD<Writable> ithColumn = data.map(new SelectColumnFunction(i));
            analyze(meta, ithColumn);
        }



        return new DataQualityAnalysis(schema,list);
    }


    public static List<Writable> sampleInvalidColumns(int count, String columnName, Schema schema, JavaRDD<Collection<Writable>> data){

        //First: filter out all valid entries, to leave only invalid entries

        int colIdx = schema.getIndexOfColumn(columnName);
        JavaRDD<Writable> ithColumn = data.map(new SelectColumnFunction(colIdx));

        ColumnMetaData meta = schema.getMetaData(columnName);

        JavaRDD<Writable> invalid = ithColumn.filter(new FilterWritablesBySchemaFunction(meta,false));

        return invalid.takeSample(false,count);
    }
}
