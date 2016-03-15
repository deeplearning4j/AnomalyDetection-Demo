package org.deeplearning4j.examples.iscx;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.schema.Schema;
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
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.data.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.examples.data.transform.normalize.Normalize;
import org.deeplearning4j.examples.misc.Histograms;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.misc.SparkUtils;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class PreprocessingISCX {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "ISCX";

    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);
    public static final String IN_DIRECTORY = DataPathUtil.REPO_BASE_DIR + "TestbedMonJun14Flows.csv"; //PATH.IN_DIR;
    public static final String OUT_DIRECTORY = PATH.REPO_BASE_DIR;
    public static final String CHART_DIRECTORY_ORIG = PATH.CHART_DIR_ORIG;
    public static final String CHART_DIRECTORY_NORM =PATH.CHART_DIR_NORM;
    protected static final boolean analysis = true;
    protected static final boolean trainSplit = true;

    public static void main(String[] args) throws Exception {

        //Get the initial schema
        Schema csvSchema = ISCXUtil.getCsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = ISCXUtil.getpreProcessingSequence();

        Schema preprocessedSchema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,dataSet + "preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("ISCX");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);
        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> processedData = executor.execute(data, seq);
        processedData.cache();

        DataQualityAnalysis dqa = null;
        DataAnalysis da = null;
        if(analysis) {
            //Analyze the quality of the columns (missing values, etc), on a per column basis
            dqa = QualityAnalyzeSpark.analyzeQuality(preprocessedSchema, processedData);
            //Do analysis, on a per-column basis
            da = AnalyzeSpark.analyze(preprocessedSchema, processedData);
        }

        DataAnalysis trainDataAnalysis = null;
        Schema normSchema = null;

        if(trainSplit) {
            //Do train/test split:
            List<JavaRDD<Collection<Writable>>> allData = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN), processedData);
            JavaRDD<Collection<Writable>> trainData = allData.get(0);
            JavaRDD<Collection<Writable>> testData = allData.get(1);
            trainDataAnalysis = AnalyzeSpark.analyze(preprocessedSchema, trainData);

            //Same normalization scheme for both. Normalization scheme based only on test data, however
            Pair<Schema, JavaRDD<Collection<Writable>>> trainDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, trainData, executor);
            Pair<Schema, JavaRDD<Collection<Writable>>> testDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, testData, executor);

            processedData.unpersist();
            trainDataNormalized.getSecond().cache();
            testDataNormalized.getSecond().cache();
            normSchema = trainDataNormalized.getFirst();
            trainDataAnalysis = AnalyzeSpark.analyze(normSchema, trainDataNormalized.getSecond());

            //Save as CSV file
            int nSplits = 1;
            SparkExport.exportCSVLocal(DataPathUtil.TRAIN_DATA_PATH, dataSet + "normalized", nSplits, ",", trainDataNormalized.getSecond(), 12345);
            SparkExport.exportCSVLocal(DataPathUtil.TEST_DATA_PATH, dataSet + "normalized", nSplits, ",", testDataNormalized.getSecond(), 12345);
            FileUtils.writeStringToFile(new File(OUT_DIRECTORY, dataSet + "normDataSchema.txt"), normSchema.toString());
        }

        sc.close();

//        List<Writable> samplesDirection = AnalyzeSpark.sampleFromColumn(100,"direction",preprocessedSchema,processedData);
//        List<Writable> samplesUnique = AnalyzeSpark.getUnique("source TCP flags",preprocessedSchema,processedData);

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);
        if(analysis) {
            System.out.println("------------------------------------------");
            System.out.println("Data quality:");
            System.out.println(dqa);
            System.out.println("------------------------------------------");
            System.out.println("Processed data summary:");
            System.out.println(da);
            System.out.println("------------------------------------------");
            System.out.println("Plot data:");
            plot(preprocessedSchema, da, CHART_DIRECTORY_ORIG);
        }

        if(trainSplit) {
            System.out.println("------------------------------------------");
            System.out.println("Normalized data summary: (train)");
            System.out.println(trainDataAnalysis);
            System.out.println("------------------------------------------");
            System.out.println("Plot normalized data:");
            plot(normSchema, trainDataAnalysis, CHART_DIRECTORY_NORM);
            System.out.println();
        }
    }

    public static Pair<Schema, JavaRDD<Collection<Writable>>> normalize(Schema schema, DataAnalysis da, JavaRDD<Collection<Writable>> input,
                                                                        SparkTransformExecutor executor) {
        TransformSequence norm = new TransformSequence.Builder(schema)
                .normalize("totalSourceBytes", Normalize.Log2Mean, da)
                .normalize("totalDestinationBytes", Normalize.Log2Mean, da)
                .normalize("totalDestinationPackets", Normalize.Log2Mean, da)
                //Do conversion of categorical fields to a set of one-hot columns, ready for network training:
                .categoricalToOneHot("direction", "protocol name", "appName", "sourceTCP_F", "destinationTCP_F")
                .transform(new CategoricalToIntegerTransform("label"))
                .build();

        Schema normSchema = norm.getFinalSchema();
        JavaRDD<Collection<Writable>> normalizedData = executor.execute(input, norm);
        return new Pair<>(normSchema, normalizedData);
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
