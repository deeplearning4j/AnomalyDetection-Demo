package org.deeplearning4j.examples.dataProcessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.berkeley.Triple;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.writable.Writable;
import org.deeplearning4j.examples.SplitTrainTestRaw;
import org.deeplearning4j.examples.datasets.iscx.ISCXUtil;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.analysis.DataAnalysis;
import org.datavec.api.transform.quality.DataQualityAnalysis;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.split.RandomSplit;
import org.datavec.spark.transform.AnalyzeSpark;
import org.datavec.spark.transform.SparkTransformExecutor;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.deeplearning4j.examples.utils.SparkConnectFactory;
import org.datavec.spark.transform.utils.SparkExport;
import org.datavec.spark.transform.utils.SparkUtils;
import org.datavec.api.transform.ui.HtmlAnalysis;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

/**Preprocessing and normalization for NB15 that does splitting of the raw data as the first step.
 * Normalization transforms are also exported, such that we can normalize on the fly (this approach is needed for
 * the UI - we need to have both the original data (for the UI) and the normalized data (for the net/predictions)
 * at the same time)
 */
public class PreprocessingPreSplit {

    public static final long RNG_SEED = 12345;
    protected static double FRACTION_TRAIN = 0.75;

    public static boolean rawSplit = true;
    public static String dataSet;
    public static DataPathUtil path;
    public static TransformProcess transformProcess = null;
    public static DataQualityAnalysis dqa;
    public static DataAnalysis dataAnalysis;
    public static DataAnalysis normDataAnalysis;
    public static Schema preprocessedSchema;
    public static Schema normSchema;
    public static JavaRDD<List<Writable>> preprocessedData;

    public static int buckets = 0;
    public static String IN_DIRECTORY;
    public static String OUT_DIRECTORY;
    public static String CHART_DIRECTORY_ORIG;
    public static String CHART_DIRECTORY_NORM;
    public static List<String> inputDir;
    public static List<String> trainTestDir;

    public static void main(String... args) throws Exception {
        setup(args[0], false);

        JavaSparkContext sc = SparkConnectFactory.getContext(dataSet);
        SparkTransformExecutor executor = new SparkTransformExecutor();

        if (rawSplit) {
            SplitTrainTestRaw.split(path.IN_DIR, path.RAW_TRAIN_FILE, path.RAW_TEST_FILE, sc);
            int i = 0;
            for (String inputPath : inputDir) {

                JavaRDD<String> rawTrainData = sc.textFile(inputPath);
                JavaRDD<List<Writable>> writableData = rawTrainData.map(new StringToWritablesFunction(new CSVRecordReader()));
                preprocessedData = executor.execute(writableData, transformProcess);
                preprocessedData.cache();

                runAnalysis(preprocessedSchema, preprocessedData);

                normalizeAndSave(i, executor, preprocessedData);
                i++;
            }
        } else {
            JavaRDD<String> rawTrainData = sc.textFile(inputDir.get(0));
            JavaRDD<List<Writable>> writableData = rawTrainData.map(new StringToWritablesFunction(new CSVRecordReader()));
            preprocessedData = executor.execute(writableData, transformProcess);
            preprocessedData.cache();

            runAnalysis(preprocessedSchema, preprocessedData);
            List<JavaRDD<List<Writable>>> split = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN), preprocessedData, RNG_SEED);

            for(int i=0; i < 2; i++) {
                normalizeAndSave(i, executor, split.get(i));
            }

        }
        printAndStoreAnalysis();

        sc.close();

    }

    public static void setup(String dS, boolean getSequenceProcess) throws Exception {
        //Pass in name of data folder
        dataSet = dS;

        //Load data squence
        transformProcess = null;

        switch (dataSet) {
            case "UNSW_NB15":
                if(getSequenceProcess) transformProcess = NB15Util.getSequencePreProcessingProcess();
                else transformProcess = NB15Util.getPreProcessingProcess();
                buckets = 30;
                break;
            case "NSLKDD":
                if(getSequenceProcess) throw new UnsupportedOperationException("Not yet implemented");
                else transformProcess = NSLKDDUtil.getPreProcessingProcess();
                buckets = 18;
                break;
            case "ISCX":
                if(getSequenceProcess) throw new UnsupportedOperationException("Not yet implemented");
                else transformProcess = ISCXUtil.getPreProcessingProcess();
                break;
        }
        if (dataSet != null) path = new DataPathUtil(dataSet);
        preprocessedSchema = defineSchema(OUT_DIRECTORY, transformProcess);
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY, "preprocessedDataSchema.txt"), preprocessedSchema.toString());
        // Paths
        inputDir = rawSplit? Arrays.asList(path.RAW_TRAIN_FILE, path.RAW_TEST_FILE): Arrays.asList(path.IN_DIR);
        trainTestDir = Arrays.asList(path.PRE_TRAIN_DATA_DIR, path.PRE_TEST_DATA_DIR);
        IN_DIRECTORY = path.IN_DIR;
        OUT_DIRECTORY = path.PRE_DIR;
        CHART_DIRECTORY_ORIG = path.CHART_DIR_ORIG;
        CHART_DIRECTORY_NORM = path.CHART_DIR_NORM;


    }

    public static void runAnalysis(Schema schema, JavaRDD<List<Writable>> data) {
        //Analyze the quality of the columns (missing values, etc), on a per column basis
        dqa = AnalyzeSpark.analyzeQuality(schema, data);

        // Per-column statis summary
        dataAnalysis = AnalyzeSpark.analyze(schema, data, buckets);
    }

    public static Schema defineSchema(String outDir, TransformProcess seq) throws IOException {
        //Get the sequence of transformations to make on the original data:
        Schema schema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(outDir, "preprocessedDataSchema.txt"), schema.toString());
        return schema;
    }

    public static void normalizeAndSave(int i, SparkTransformExecutor executor, JavaRDD<List<Writable>> data) throws Exception{
        Triple<TransformProcess, Schema, JavaRDD<List<Writable>>> dataNormalized;

        switch (dataSet) {
            case "UNSW_NB15":
                dataNormalized = NB15Util.normalize(preprocessedSchema, dataAnalysis, data, executor);
                break;
            case "NSLKDD":
                dataNormalized = NSLKDDUtil.normalize(preprocessedSchema, dataAnalysis, data, executor);
                break;
            case "ISCX":
                dataNormalized = ISCXUtil.normalize(preprocessedSchema, dataAnalysis, data, executor);
                break;
            default:
                throw new RuntimeException("Unknown data set: " + dataSet);
        }

        dataNormalized.getThird().cache();
        normSchema = dataNormalized.getSecond();
        runAnalysis(normSchema, dataNormalized.getThird());

        SparkExport.exportCSVLocal(trainTestDir.get(i), i + dataSet + "normalized", 1, ",", dataNormalized.getThird());

        if (i == 0) {
            saveNormSchemaTransform(dataNormalized.getFirst());
        }

        preprocessedData.unpersist();
        dataNormalized.getThird().unpersist();
    }

    public static void printAnalysis(Object analysis, String tag) {
        //Wait for spark to stop its console spam before printing analysis
        System.out.println("------------------------------------------");
        System.out.println(tag);
        System.out.println(analysis);
    }

    public static void printAndStoreAnalysis() throws Exception {
        //Print analysis
        Thread.sleep(200);
        printAnalysis(dqa, "Data quality:");
        printAnalysis(dataAnalysis, "Processed data summary:");
        printAnalysis(normDataAnalysis, "Normalized data summary:");

        //Store histograms
        System.out.println("Storing charts...");
//        Histograms.plot(preprocessedSchema, dataAnalysis, CHART_DIRECTORY_ORIG);
//        Histograms.plot(normSchema, normDataAnalysis, CHART_DIRECTORY_NORM);
        System.out.println();

        try{
            HtmlAnalysis.createHtmlAnalysisFile(dataAnalysis, new File(FilenameUtils.concat(path.CHART_DIR_ORIG,"orig_analysis.html")));
            HtmlAnalysis.createHtmlAnalysisFile(normDataAnalysis, new File(FilenameUtils.concat(path.CHART_DIR_NORM,"norm_analysis.html")));
        }catch(Exception e ){
            e.printStackTrace();
        }
    }

    public static void saveNormSchemaTransform(TransformProcess transform) throws Exception{
        FileUtils.writeStringToFile(new File(path.NORM_SCHEMA), normSchema.toString());
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(path.NORMALIZER_FILE)))) {
            oos.writeObject(transform);
        }
    }
    public static void storeAws(){
            // load preprocessed
//            S3Uploader s3Up = new S3Uploader();
//            MultipleFileUpload mlpUp = s3Up.uploadFolder(s3Bucket, s3KeyPrefixIn, new File(System.getProperty("user.home") + outputFilePath), true);
//            mlpUp.waitForCompletion();
        }

    public static void pullAws(){
        // pull down raw
//            S3Downloader s3Down = new S3Downloader();
//            MultipleFileDownload mlpDown = s3Down.downloadFolder(s3Bucket, s3KeyPrefixOut, new File(System.getProperty("user.home") + inputFilePath));
//            mlpDown.waitForCompletion();

    }


}
