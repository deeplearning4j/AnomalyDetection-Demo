package org.deeplearning4j.examples.data.analysis;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.analysis.columns.*;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.*;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.integer.IntegerAnalysisAddFunction;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.integer.IntegerAnalysisCounter;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.integer.IntegerAnalysisMergeFunction;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.longa.LongAnalysisAddFunction;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.longa.LongAnalysisCounter;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.longa.LongAnalysisMergeFunction;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.real.RealAnalysisAddFunction;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.real.RealAnalysisCounter;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.real.RealAnalysisMergeFunction;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 4/03/2016.
 */
public class AnalyzeSpark {

    public static final int DEFAULT_HISTOGRAM_BUCKETS = 30;

    public static DataAnalysis analyze(Schema schema, JavaRDD<Collection<Writable>> data) {
        return analyze(schema,data,DEFAULT_HISTOGRAM_BUCKETS);
    }

    public static DataAnalysis analyze(Schema schema, JavaRDD<Collection<Writable>> data, int maxHistogramBuckets) {

        data.cache();

        int nColumns = schema.numColumns();

        //This is inefficient, but it's easy to implement. Good enough for now!
        List<ColumnAnalysis> list = new ArrayList<>(nColumns);
        for( int i=0; i<nColumns; i++ ){

            String columnName = schema.getName(i);
            ColumnType type = schema.getType(i);

            JavaRDD<Writable> ithColumn = data.map(new SelectColumnFunction(i));
            ithColumn.cache();

            switch(type){
                case String:

                    ithColumn.cache();
                    long countUnique = ithColumn.distinct().count();

                    JavaDoubleRDD stringLength = ithColumn.mapToDouble(new StringLengthFunction());
                    StatCounter stringLengthStats = stringLength.stats();

                    long min = (int)stringLengthStats.min();
                    long max = (int)stringLengthStats.max();

                    long nBuckets = max-min+1;

                    Tuple2<double[],long[]> hist;
                    if(max == min){
                        //Edge case that spark doesn't like
                        hist = new Tuple2<>(new double[]{min},new long[]{stringLengthStats.count()});
                    } else if(nBuckets < maxHistogramBuckets){
                        hist = stringLength.histogram((int)nBuckets);
                    } else {
                        hist = stringLength.histogram(maxHistogramBuckets);
                    }

                    list.add(new StringAnalysis(countUnique,(int)min,(int)max,stringLengthStats.mean(),
                            stringLengthStats.sampleStdev(),stringLengthStats.sampleVariance(),stringLengthStats.count(),
                            hist._1(),hist._2()));


                    break;
                case Integer:
                    JavaDoubleRDD doubleRDD1 = ithColumn.mapToDouble(new WritableToDoubleFunction());
                    StatCounter stats1 = doubleRDD1.stats();

                    //Now: count number of 0, >0, <0

                    IntegerAnalysisCounter counter = new IntegerAnalysisCounter();
                    counter = ithColumn.aggregate(counter,new IntegerAnalysisAddFunction(),new IntegerAnalysisMergeFunction());

                    long min1 = (int)stats1.min();
                    long max1 = (int)stats1.max();

                    long nBuckets1 = max1-min1+1;

                    Tuple2<double[],long[]> hist1;
                    if(max1 == min1){
                        //Edge case that spark doesn't like
                        hist1 = new Tuple2<>(new double[]{min1},new long[]{stats1.count()});
                    } else if(nBuckets1 < maxHistogramBuckets){
                        hist1 = doubleRDD1.histogram((int)nBuckets1);
                    } else {
                        hist1 = doubleRDD1.histogram(maxHistogramBuckets);
                    }

                    list.add(new IntegerAnalysis((int)stats1.min(),(int)stats1.max(),stats1.mean(),stats1.sampleStdev(),stats1.sampleVariance(),
                            counter.getCountZero(),counter.getCountNegative(),counter.getCountPositive(),stats1.count(),hist1._1(),hist1._2()));

                    break;
                case Long:
                    JavaDoubleRDD doubleRDDLong = ithColumn.mapToDouble(new WritableToDoubleFunction());
                    StatCounter statsLong = doubleRDDLong.stats();

                    LongAnalysisCounter counterL = new LongAnalysisCounter();
                    counterL = ithColumn.aggregate(counterL,new LongAnalysisAddFunction(),new LongAnalysisMergeFunction());

                    long minLong = (long)statsLong.min();
                    long maxLong = (long)statsLong.max();

                    long nBucketsLong = maxLong-minLong+1;

                    Tuple2<double[],long[]> histLong;
                    if(maxLong == minLong){
                        //Edge case that spark doesn't like
                        hist1 = new Tuple2<>(new double[]{minLong},new long[]{statsLong.count()});
                    } else if(nBucketsLong < maxHistogramBuckets){
                        hist1 = doubleRDDLong.histogram((int)nBucketsLong);
                    } else {
                        hist1 = doubleRDDLong.histogram(maxHistogramBuckets);
                    }

                    list.add(new LongAnalysis((long)statsLong.min(),(long)statsLong.max(),statsLong.mean(),statsLong.sampleStdev(),
                            statsLong.sampleVariance(),counterL.getCountZero(),counterL.getCountNegative(),counterL.getCountPositive(),
                            statsLong.count(),hist1._1(),hist1._2()));

                    break;
                case Double:
                    JavaDoubleRDD doubleRDD = ithColumn.mapToDouble(new WritableToDoubleFunction());
                    StatCounter stats = doubleRDD.stats();

                    RealAnalysisCounter counterR = new RealAnalysisCounter();
                    counterR = ithColumn.aggregate(counterR,new RealAnalysisAddFunction(),new RealAnalysisMergeFunction());

                    long min2 = (int)stats.min();
                    long max2 = (int)stats.max();

                    Tuple2<double[],long[]> hist2;
                    if(max2 == min2){
                        //Edge case that spark doesn't like
                        hist2 = new Tuple2<>(new double[]{min2},new long[]{stats.count()});
                    } else {
                        hist2 = doubleRDD.histogram(maxHistogramBuckets);
                    }

                    list.add(new RealAnalysis(stats.min(),stats.max(),stats.mean(),stats.sampleStdev(),stats.sampleVariance(),
                            counterR.getCountZero(),counterR.getCountNegative(),counterR.getCountPositive(),stats.count(),
                            hist2._1(),hist2._2()));
                    break;
                case Categorical:

                    JavaRDD<String> rdd = ithColumn.map(new WritableToStringFunction());
                    Map<String,Long> map = rdd.countByValue();

                    list.add(new CategoricalAnalysis(map));


                    break;
                case BLOB:
                    list.add(new BlobAnalysis());
                    break;
            }

            ithColumn.unpersist();
        }


        return new DataAnalysis(schema,list);
    }

    public static List<Writable> sampleFromColumn(int count, String columnName, Schema schema, JavaRDD<Collection<Writable>> data){

        int colIdx = schema.getIndexOfColumn(columnName);
        JavaRDD<Writable> ithColumn = data.map(new SelectColumnFunction(colIdx));

        return ithColumn.takeSample(false,count);
    }

}
