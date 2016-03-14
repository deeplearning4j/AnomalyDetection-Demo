package org.deeplearning4j.examples.nslkdd;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.DataPath;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.TransformSequence;
import org.deeplearning4j.examples.data.analysis.AnalyzeSpark;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.IntegerAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.LongAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.RealAnalysis;
import org.deeplearning4j.examples.data.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.dataquality.QualityAnalyzeSpark;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.misc.Histograms;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.nb15.NB15Util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.List;

/**
 *
 */
@SuppressWarnings("Duplicates")
public class PreprocessingNSLKDDPreSplit {

    protected static String dataSet = "NSL_KDD";
    protected static final DataPath PATH = new DataPath(dataSet);
    public static final String RAW_TRAIN_DATA_FILE = PATH.RAW_TRAIN_PATH;
    public static final String OUT_DIRECTORY = PATH.PRE_DIR;
    public static final String CHART_DIRECTORY_ORIG = PATH.CHART_DIR_ORIG;
    public static final String CHART_DIRECTORY_NORM = PATH.CHART_DIR_NORM;

    public static void main(String[] args) throws Exception {

        //Get the sequence of transformations to make on the original data:
        TransformSequence seq = NSLKDDUtil.getNLSKDDPreProcessingSequence();

        Schema preprocessedSchema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawTrainData = sc.textFile(RAW_TRAIN_DATA_FILE);
        JavaRDD<Collection<Writable>> writableTrainData = rawTrainData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> preprocessedTrainData = executor.execute(writableTrainData, seq);
        preprocessedTrainData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQuality(preprocessedSchema, preprocessedTrainData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyze(preprocessedSchema, preprocessedTrainData);
        DataAnalysis trainDataAnalysis = AnalyzeSpark.analyze(preprocessedSchema, preprocessedTrainData);

        //Same normalization scheme for both. Normalization scheme based only on test data, however
        Triple<TransformSequence, Schema, JavaRDD<Collection<Writable>>> trainDataNormalized = NSLKDDUtil.normalize(preprocessedSchema, trainDataAnalysis, preprocessedTrainData, executor);

        preprocessedTrainData.unpersist();
        trainDataNormalized.getThird().cache();
        Schema normSchema = trainDataNormalized.getSecond();

        DataAnalysis trainDataAnalyis = AnalyzeSpark.analyze(normSchema, trainDataNormalized.getThird());

        //Save normalized training data as CSV file
        int nSplits = 1;
        SparkExport.exportCSVLocal(DataPath.TRAIN_DATA_PATH, dataSet + "normalized", nSplits, ",", trainDataNormalized.getThird(), 12345);
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"normDataSchema.txt"),normSchema.toString());

        //Save the normalizer transform sequence:
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(PATH.NORMALIZER_FILE)))){
            oos.writeObject(trainDataNormalized.getFirst());
        }

        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");
        System.out.println("Data quality:");
        System.out.println(dqa);

        System.out.println("------------------------------------------");

        System.out.println("Processed data summary:");
        System.out.println(da);

        System.out.println("------------------------------------------");

        System.out.println("Normalized data summary: (train)");
        System.out.println(trainDataAnalyis);

        //analysis and histograms
        plot(preprocessedSchema, da, CHART_DIRECTORY_ORIG);
        plot(normSchema, trainDataAnalyis, CHART_DIRECTORY_NORM);

        System.out.println();
    }

    public static void plot(Schema finalSchema, DataAnalysis da, String directory) throws Exception {
        //Plots!
        List<ColumnAnalysis> analysis = da.getColumnAnalysis();
        List<String> names = finalSchema.getColumnNames();
        List<ColumnType> types = finalSchema.getColumnTypes();

        for (int i = 0; i < analysis.size(); i++) {
            ColumnType type = types.get(i);
            ColumnAnalysis a = analysis.get(i);
            double[] bins;
            long[] counts;
            switch (type) {
                case Integer:
                    IntegerAnalysis ia = (IntegerAnalysis) a;
                    bins = ia.getHistogramBuckets();
                    counts = ia.getHistogramBucketCounts();
                    break;
                case Long:
                    LongAnalysis la = (LongAnalysis) a;
                    bins = la.getHistogramBuckets();
                    counts = la.getHistogramBucketCounts();
                    break;
                case Double:
                    RealAnalysis ra = (RealAnalysis) a;
                    bins = ra.getHistogramBuckets();
                    counts = ra.getHistogramBucketCounts();
                    break;
                default:
                    continue;
            }

            String colName = names.get(i);


//            Histograms.plot(bins,counts,colName);
            File f = new File(directory, colName + ".png");
            if (f.exists()) f.delete();
            Histograms.exportHistogramImage(f, bins, counts, colName, 1000, 650);
        }


    }

}
