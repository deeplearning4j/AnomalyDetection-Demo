package org.deeplearning4j.examples.dataProcessing.spark;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.analysis.DataAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.analysis.SequenceDataAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.analysis.columns.*;
import org.deeplearning4j.examples.dataProcessing.api.analysis.sequence.SequenceLengthAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.*;
import org.deeplearning4j.examples.dataProcessing.api.metadata.*;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.seqlength.IntToDoubleFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.*;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.seqlength.SequenceLengthAnalysisAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.seqlength.SequenceLengthAnalysisCounter;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.seqlength.SequenceLengthAnalysisMergeFunction;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.integer.IntegerAnalysisAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.integer.IntegerAnalysisCounter;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.integer.IntegerAnalysisMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.longa.LongAnalysisAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.longa.LongAnalysisCounter;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.longa.LongAnalysisMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.real.RealAnalysisAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.real.RealAnalysisCounter;
import org.deeplearning4j.examples.dataProcessing.spark.analysis.real.RealAnalysisMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.filter.FilterWritablesBySchemaFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.categorical.CategoricalQualityAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.categorical.CategoricalQualityMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.integer.IntegerQualityAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.integer.IntegerQualityMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.longq.LongQualityAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.longq.LongQualityMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.real.RealQualityAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.real.RealQualityMergeFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.string.StringQualityAddFunction;
import org.deeplearning4j.examples.dataProcessing.spark.quality.string.StringQualityMergeFunction;
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

    public static SequenceDataAnalysis analyzeSequence(Schema schema, JavaRDD<Collection<Collection<Writable>>> data) {
        return analyzeSequence(schema,data,DEFAULT_HISTOGRAM_BUCKETS);
    }

    public static SequenceDataAnalysis analyzeSequence(Schema schema, JavaRDD<Collection<Collection<Writable>>> data, int maxHistogramBuckets) {
        data.cache();
        JavaRDD<Collection<Writable>> fmSeq = data.flatMap(new SequenceFlatMapFunction());
        DataAnalysis da = analyze(schema, fmSeq);
        //Analyze the length of the sequences:
        JavaRDD<Integer> seqLengths = data.map(new SequenceLengthFunction());
        seqLengths.cache();
        SequenceLengthAnalysisCounter counter = new SequenceLengthAnalysisCounter();
        counter = seqLengths.aggregate(counter,new SequenceLengthAnalysisAddFunction(), new SequenceLengthAnalysisMergeFunction());

        int max = counter.getMaxLengthSeen();
        int min = counter.getMinLengthSeen();
        int nBuckets = counter.getMaxLengthSeen() - counter.getMinLengthSeen();

        Tuple2<double[],long[]> hist;
        if(max == min){
            //Edge case that spark doesn't like
            hist = new Tuple2<>(new double[]{min},new long[]{counter.getCountTotal()});
        } else if(nBuckets < maxHistogramBuckets){
            JavaDoubleRDD drdd = seqLengths.mapToDouble(new IntToDoubleFunction());
            hist = drdd.histogram(nBuckets);
        } else {
            JavaDoubleRDD drdd = seqLengths.mapToDouble(new IntToDoubleFunction());
            hist = drdd.histogram(maxHistogramBuckets);
        }
        seqLengths.unpersist();


        SequenceLengthAnalysis lengthAnalysis = SequenceLengthAnalysis.builder()
                .totalNumSequences(counter.getCountTotal())
                .minSeqLength(counter.getMinLengthSeen())
                .maxSeqLength(counter.getMaxLengthSeen())
                .countZeroLength(counter.getCountZeroLength())
                .countOneLength(counter.getCountOneLength())
                .meanLength(counter.getMean())
                .histogramBuckets(hist._1())
                .histogramBucketCounts(hist._2())
                .build();

        return new SequenceDataAnalysis(schema,da.getColumnAnalysis(),lengthAnalysis);
    }


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

                    IntegerAnalysis ia = IntegerAnalysis.builder()
                            .min((int)stats1.min())
                            .max((int)stats1.max())
                            .mean(stats1.mean())
                            .sampleStdev(stats1.sampleStdev())
                            .sampleVariance(stats1.sampleVariance())
                            .countZero(counter.getCountZero())
                            .countNegative(counter.getCountNegative())
                            .countPositive(counter.getCountPositive())
                            .countMinValue(counter.getCountMinValue())
                            .countMaxValue(counter.getCountMaxValue())
                            .countTotal(stats1.count())
                            .histogramBuckets(hist1._1())
                            .histogramBucketCounts(hist1._2()).build();

                    list.add(ia);
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
                        histLong = new Tuple2<>(new double[]{minLong},new long[]{statsLong.count()});
                    } else if(nBucketsLong < maxHistogramBuckets){
                        histLong = doubleRDDLong.histogram((int)nBucketsLong);
                    } else {
                        histLong = doubleRDDLong.histogram(maxHistogramBuckets);
                    }

                    LongAnalysis la = LongAnalysis.builder()
                            .min((long)statsLong.min())
                            .max((long)statsLong.max())
                            .mean(statsLong.mean())
                            .sampleStdev(statsLong.sampleStdev())
                            .sampleVariance(statsLong.sampleVariance())
                            .countZero(counterL.getCountZero())
                            .countNegative(counterL.getCountNegative())
                            .countPositive(counterL.getCountPositive())
                            .countMinValue(counterL.getCountMinValue())
                            .countMaxValue(counterL.getCountMaxValue())
                            .countTotal(statsLong.count())
                            .histogramBuckets(histLong._1())
                            .histogramBucketCounts(histLong._2()).build();

                    list.add(la);

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

                    RealAnalysis ra = RealAnalysis.builder()
                            .min(stats.min())
                            .max(stats.max())
                            .mean(stats.mean())
                            .sampleStdev(stats.sampleStdev())
                            .sampleVariance(stats.sampleVariance())
                            .countZero(counterR.getCountZero())
                            .countNegative(counterR.getCountNegative())
                            .countPositive(counterR.getCountPositive())
                            .countMinValue(counterR.getCountMinValue())
                            .countMaxValue(counterR.getCountMaxValue())
                            .countTotal(stats.count())
                            .histogramBuckets(hist2._1())
                            .histogramBucketCounts(hist2._2()).build();

                    list.add(ra);

//                    list.add(new RealAnalysis(stats.min(),stats.max(),stats.mean(),stats.sampleStdev(),stats.sampleVariance(),
//                            counterR.getCountZero(),counterR.getCountNegative(),counterR.getCountPositive(),stats.count(),
//                            hist2._1(),hist2._2()));
                    break;
                case Categorical:

                    JavaRDD<String> rdd = ithColumn.map(new WritableToStringFunction());
                    Map<String,Long> map = rdd.countByValue();

                    list.add(new CategoricalAnalysis(map));


                    break;
                case Bytes:
                    list.add(new BytesAnalysis());
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

    public static List<Writable> getUnique(String columnName, Schema schema, JavaRDD<Collection<Writable>> data){
        int colIdx = schema.getIndexOfColumn(columnName);
        JavaRDD<Writable> ithColumn = data.map(new SelectColumnFunction(colIdx));

        return ithColumn.distinct().collect();
    }

    public static List<Collection<Writable>> sample(int count, JavaRDD<Collection<Writable>> data){
        return data.takeSample(false,count);
    }

    public static List<Collection<Collection<Writable>>> sampleSequence(int count, JavaRDD<Collection<Collection<Writable>>> data ){
        return data.takeSample(false,count);
    }





    private static ColumnQuality analyze(ColumnMetaData meta, JavaRDD<Writable> ithColumn){

        switch(meta.getColumnType()){
            case String:
                ithColumn.cache();
                long countUnique = ithColumn.distinct().count();

                StringQuality initialString = new StringQuality();
                StringQuality stringQuality = ithColumn.aggregate(initialString,new StringQualityAddFunction((StringMetaData)meta),new StringQualityMergeFunction());
                return stringQuality.add(new StringQuality(0,0,0,0,0,0,0,0,0,countUnique));
            case Integer:
                IntegerQuality initialInt = new IntegerQuality(0,0,0,0,0);
                return ithColumn.aggregate(initialInt,new IntegerQualityAddFunction((IntegerMetaData)meta),new IntegerQualityMergeFunction());
            case Long:
                LongQuality initialLong = new LongQuality();
                return ithColumn.aggregate(initialLong,new LongQualityAddFunction((LongMetaData)meta),new LongQualityMergeFunction());
            case Double:
                RealQuality initialReal = new RealQuality();
                return ithColumn.aggregate(initialReal,new RealQualityAddFunction((DoubleMetaData)meta), new RealQualityMergeFunction());
            case Categorical:
                CategoricalQuality initialCat = new CategoricalQuality();
                return ithColumn.aggregate(initialCat,new CategoricalQualityAddFunction((CategoricalMetaData)meta),new CategoricalQualityMergeFunction());
            case Time:
                throw new UnsupportedOperationException("Not yet implemented");
            case Bytes:
                return new BytesQuality();    //TODO
            default:
                throw new RuntimeException("Unknown or not implemented column type: " + meta.getColumnType());
        }
    }

    public static DataQualityAnalysis analyzeQualitySequence(Schema schema, JavaRDD<Collection<Collection<Writable>>> data){
        JavaRDD<Collection<Writable>> fmSeq = data.flatMap(new SequenceFlatMapFunction());
        return analyzeQuality(schema, fmSeq);
    }

    public static DataQualityAnalysis analyzeQuality(Schema schema, JavaRDD<Collection<Writable>> data){

        data.cache();
        int nColumns = schema.numColumns();

        //This is inefficient, but it's easy to implement. Good enough for now!
        List<ColumnQuality> list = new ArrayList<>(nColumns);

        for( int i=0; i<nColumns; i++ ) {
            ColumnMetaData meta = schema.getMetaData(i);
            JavaRDD<Writable> ithColumn = data.map(new SelectColumnFunction(i));
            list.add(analyze(meta, ithColumn));
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
