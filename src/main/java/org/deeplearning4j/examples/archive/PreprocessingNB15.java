package org.deeplearning4j.examples.archive;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.preprocessing.api.TransformProcess;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.preprocessing.api.split.RandomSplit;
import org.deeplearning4j.preprocessing.api.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.preprocessing.api.charts.Histograms;
import org.deeplearning4j.preprocessing.spark.AnalyzeSpark;
import org.deeplearning4j.preprocessing.api.analysis.DataAnalysis;
import org.deeplearning4j.preprocessing.api.dataquality.DataQualityAnalysis;
import org.deeplearning4j.preprocessing.spark.SparkTransformExecutor;
import org.deeplearning4j.preprocessing.spark.misc.StringToWritablesFunction;
import org.deeplearning4j.preprocessing.api.transform.normalize.Normalize;
import org.deeplearning4j.preprocessing.spark.utils.SparkExport;
import org.deeplearning4j.preprocessing.spark.utils.SparkUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.*;

/**
 * Created by Alex on 5/03/2016.
 */
public class PreprocessingNB15 {

    public static final long RNG_SEED = 12345;
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
        TransformProcess seq = NB15Util.getPreProcessingProcess();

        Schema preprocessedSchema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(PATH.OUT_DIR,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(PATH.IN_DIR);

        JavaRDD<List<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<List<Writable>> processedData = executor.execute(data, seq);
        processedData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = AnalyzeSpark.analyzeQuality(preprocessedSchema, processedData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyze(preprocessedSchema, processedData);

        //Do train/test split:
        List<JavaRDD<List<Writable>>> allData = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN), processedData, RNG_SEED);
        JavaRDD<List<Writable>> trainData = allData.get(0);
        JavaRDD<List<Writable>> testData = allData.get(1);

        DataAnalysis trainDataAnalysis = AnalyzeSpark.analyze(preprocessedSchema, trainData);

        //Same normalization scheme for both. Normalization scheme based only on test data, however
        Triple<TransformProcess, Schema, JavaRDD<List<Writable>>> trainDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, trainData, executor);
        Triple<TransformProcess, Schema, JavaRDD<List<Writable>>> testDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, testData, executor);

        processedData.unpersist();
        trainDataNormalized.getThird().cache();
        testDataNormalized.getThird().cache();
        Schema normSchema = trainDataNormalized.getSecond();


        DataAnalysis trainDataAnalyis = AnalyzeSpark.analyze(normSchema, trainDataNormalized.getThird());

        //Save as CSV file
        SparkExport.exportCSVLocal(new File(PATH.NORM_TRAIN_DATA_FILE), ",", trainDataNormalized.getThird(), 12345);
        SparkExport.exportCSVLocal(new File(PATH.NORM_TEST_DATA_FILE), ",", testDataNormalized.getThird(), 12345);
        FileUtils.writeStringToFile(new File(PATH.OUT_DIR,"normDataSchema.txt"),normSchema.toString());

        //Save the normalizer transform sequence:
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

    public static Triple<TransformProcess, Schema, JavaRDD<List<Writable>>>
                normalize(Schema schema, DataAnalysis da, JavaRDD<List<Writable>> input, SparkTransformExecutor executor) {
        TransformProcess norm = new TransformProcess.Builder(schema)
                .normalize("source port", Normalize.MinMax, da)
                .normalize("destination port", Normalize.MinMax, da)
                .normalize("total duration", Normalize.Log2Mean, da)
                .normalize("source-dest bytes", Normalize.Log2Mean, da)
                .normalize("dest-source bytes", Normalize.Log2Mean, da)
                .normalize("source-dest time to live", Normalize.MinMax, da)
                .normalize("dest-source time to live", Normalize.MinMax, da)
                .normalize("source packets lost", Normalize.Log2Mean, da)
                .normalize("destination packets lost", Normalize.Log2Mean, da)
                .normalize("source bits per second", Normalize.Log2Mean, da)
                .normalize("destination bits per second", Normalize.Log2Mean, da)
                .normalize("source-destination packet count", Normalize.Log2Mean, da)
                .normalize("dest-source packet count", Normalize.Log2Mean, da)
                .normalize("source TCP window adv", Normalize.MinMax, da)           //raw data: 0 or 255 -> 0 or 1
                .normalize("dest TCP window adv", Normalize.MinMax, da)
                .normalize("source mean flow packet size", Normalize.Log2Mean, da)
                .normalize("dest mean flow packet size", Normalize.Log2Mean, da)
                .normalize("transaction pipelined depth", Normalize.Log2MeanExcludingMin, da)   //2.33M are 0
                .normalize("content size", Normalize.Log2Mean, da)

                .normalize("source jitter ms", Normalize.Log2MeanExcludingMin, da)      //963k are 0
                .normalize("dest jitter ms", Normalize.Log2MeanExcludingMin, da)        //900k are 0
                .normalize("source interpacket arrival time", Normalize.Log2MeanExcludingMin, da)       //OK, but just to keep in line with the below
                .normalize("destination interpacket arrival time", Normalize.Log2MeanExcludingMin, da)  //500k are 0
                .normalize("tcp setup round trip time", Normalize.Log2MeanExcludingMin, da)     //1.05M are 0
                .normalize("tcp setup time syn syn_ack", Normalize.Log2MeanExcludingMin, da)    //1.05M are 0
                .normalize("tcp setup time syn_ack ack", Normalize.Log2MeanExcludingMin, da)    //1.06M are 0
                .normalize("count time to live", Normalize.MinMax, da)  //0 to 6 in data
                .normalize("count flow http methods", Normalize.Log2MeanExcludingMin, da) //0 to 37; vast majority (2.33M of 2.54M) are 0
                .normalize("count ftp commands", Normalize.MinMax, da)  //0 to 8; only 43k are non-zero
                .normalize("count same service and source", Normalize.Log2Mean, da)
                .normalize("count same service and dest", Normalize.Log2Mean, da)
                .normalize("count same dest", Normalize.Log2Mean, da)
                .normalize("count same source", Normalize.Log2Mean, da)
                .normalize("count same source addr dest port", Normalize.Log2MeanExcludingMin, da)              //1.69M ore the min value of 1.0
                .normalize("count same dest addr source port", Normalize.Log2MeanExcludingMin, da) //1.97M of 2.54M are the minimum value of 1.0
                .normalize("count same source dest address", Normalize.Log2Mean, da)

                //Do conversion of categorical fields to a set of one-hot columns, ready for network training:
                .categoricalToOneHot("transaction protocol", "state", "service", "equal ips and ports", "is ftp login")
                .transform(new CategoricalToIntegerTransform("attack category"))
                .build();

        Schema normSchema = norm.getFinalSchema();
        JavaRDD<List<Writable>> normalizedData = executor.execute(input, norm);
        return new Triple<>(norm, normSchema, normalizedData);
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
//                    DoubleAnalysis ra = (DoubleAnalysis) a;
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
//    }

}
