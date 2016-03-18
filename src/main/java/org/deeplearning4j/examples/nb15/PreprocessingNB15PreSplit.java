package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.TransformProcess;
import org.deeplearning4j.examples.data.api.split.RandomSplit;
import org.deeplearning4j.examples.misc.SparkUtils;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.data.spark.AnalyzeSpark;
import org.deeplearning4j.examples.data.api.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.api.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.spark.SparkTransformExecutor;
import org.deeplearning4j.examples.data.api.schema.Schema;
import org.deeplearning4j.examples.data.spark.misc.StringToWritablesFunction;
import org.deeplearning4j.examples.misc.Histograms;
import org.deeplearning4j.examples.misc.SparkExport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.List;

/**Preprocessing and normalization for NB15 that does splitting of the raw data as the first step.
 * Normalization transforms are also exported, such that we can normalize on the fly (this approach is needed for
 * the UI - we need to have both the original data (for the UI) and the normalized data (for the net/predictions)
 * at the same time)
 *
 */
@SuppressWarnings("Duplicates")
public class PreprocessingNB15PreSplit {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "UNSW_NB15";
    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);

    public static void main(String[] args) throws Exception {
        // For AWS
        if(DataPathUtil.AWS) {
            // pull down raw
//            S3Downloader s3Down = new S3Downloader();
//            MultipleFileDownload mlpDown = s3Down.downloadFolder(s3Bucket, s3KeyPrefixOut, new File(System.getProperty("user.home") + inputFilePath));
//            mlpDown.waitForCompletion();
        }

        //Get the sequence of transformations to make on the original data:
        TransformProcess seq = NB15Util.getNB15PreProcessingSequence();

        Schema preprocessedSchema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(PATH.OUT_DIR,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //First: let's take the raw data, and split it
        JavaRDD<String> rawData = sc.textFile(PATH.IN_DIR);

        List<JavaRDD<String>> split = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN),rawData);
        SparkExport.exportStringLocal(new File(PATH.RAW_TRAIN_PATH),split.get(0),12345);
        SparkExport.exportStringLocal(new File(PATH.RAW_TEST_PATH),split.get(1),12345);

        //Now that split is done: do preprocessing and normalization on training data
        JavaRDD<String> rawTrainData = sc.textFile(PATH.RAW_TRAIN_PATH);
        JavaRDD<Collection<Writable>> writableTrainData = rawTrainData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> preprocessedTrainData = executor.execute(writableTrainData, seq);
        preprocessedTrainData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = AnalyzeSpark.analyzeQuality(preprocessedSchema, preprocessedTrainData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyze(preprocessedSchema, preprocessedTrainData);
        DataAnalysis trainDataAnalysis = AnalyzeSpark.analyze(preprocessedSchema, preprocessedTrainData);

        //Same normalization scheme for both. Normalization scheme based only on test data, however
        Triple<TransformProcess, Schema, JavaRDD<Collection<Writable>>> trainDataNormalized = NB15Util.normalize(preprocessedSchema, trainDataAnalysis, preprocessedTrainData, executor);

        preprocessedTrainData.unpersist();
        trainDataNormalized.getThird().cache();
        Schema normSchema = trainDataNormalized.getSecond();

        DataAnalysis trainDataAnalyis = AnalyzeSpark.analyze(normSchema, trainDataNormalized.getThird());

        //Save normalized training data as CSV file
        SparkExport.exportCSVLocal(new File(PATH.TRAIN_DATA_FILE), ",", trainDataNormalized.getThird(), 12345);
        FileUtils.writeStringToFile(new File(PATH.OUT_DIR,"normalizedDataSchema.txt"),normSchema.toString());

        //Save the normalizer transform sequence. We'll use this later to normalize our data on-the-fly
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(PATH.NORMALIZER_FILE)))){
            oos.writeObject(trainDataNormalized.getFirst());
        }
        sc.close();


        if(DataPathUtil.AWS) {
            // load preprocessed
            throw new UnsupportedOperationException();
//            S3Uploader s3Up = new S3Uploader();
//            MultipleFileUpload mlpUp = s3Up.uploadFolder(s3Bucket, s3KeyPrefixIn, new File(System.getProperty("user.home") + outputFilePath), true);
//            mlpUp.waitForCompletion();
        }

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
        Histograms.exportPlots(preprocessedSchema, da, PATH.CHART_DIR_ORIG);
        Histograms.exportPlots(normSchema, trainDataAnalyis, PATH.CHART_DIR_NORM);
    }

//    public static void plot(Schema finalSchema, DataAnalysis da, String directory) throws Exception {
//        //Plots!
//        List<ColumnAnalysis> analysis = da.getColumnAnalysis();
//        List<String> names = finalSchema.getColumnNames();
//        List<ColumnType> types = finalSchema.getColumnTypes();
//
//        for (int i = 0; i < analysis.size(); i++) {
//            ColumnType type = types.get(i);
//            ColumnAnalysis a = analysis.get(i);
//            double[] bins;
//            long[] counts;
//            switch (type) {
//                case Integer:
//                    IntegerAnalysis ia = (IntegerAnalysis) a;
//                    bins = ia.getHistogramBuckets();
//                    counts = ia.getHistogramBucketCounts();
//                    break;
//                case Long:
//                    LongAnalysis la = (LongAnalysis) a;
//                    bins = la.getHistogramBuckets();
//                    counts = la.getHistogramBucketCounts();
//                    break;
//                case Double:
//                    RealAnalysis ra = (RealAnalysis) a;
//                    bins = ra.getHistogramBuckets();
//                    counts = ra.getHistogramBucketCounts();
//                    break;
//                default:
//                    continue;
//            }
//
//            String colName = names.get(i);
//
//
////            Histograms.plot(bins,counts,colName);
//            File f = new File(directory, colName + ".png");
//            if (f.exists()) f.delete();
//            Histograms.exportHistogramImage(f, bins, counts, colName, 1000, 650);
//        }
//
//
//    }

}
