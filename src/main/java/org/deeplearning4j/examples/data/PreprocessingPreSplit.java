package org.deeplearning4j.examples.data;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.data.analysis.AnalyzeSpark;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.dataquality.QualityAnalyzeSpark;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.misc.Histograms;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.nslkdd.NSLKDDUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class PreprocessingPreSplit {

    public static void main(String[] args) throws Exception {
        TransformSequence seq = NSLKDDUtil.getpreProcessingSequence(); // haven't figured out how to pass in
        String dataSet =  args[0]; //"NSL_KDD";

        DataPathUtil path = new DataPathUtil(dataSet);
        List<String> inputDir = Arrays.asList(path.RAW_TRAIN_PATH, path.RAW_TEST_PATH);
        List<String> trainTestDir = Arrays.asList(DataPathUtil.TRAIN_DATA_PATH, DataPathUtil.TEST_DATA_PATH);
        String outDir = path.PRE_DIR;
        String chartDirOrig = path.CHART_DIR_ORIG;
        String chartDirNorm = path.CHART_DIR_NORM;

        JavaRDD<Collection<Writable>> preprocessedData;
        Schema preprocessedSchema = defineSchema(outDir, seq);
        JavaSparkContext sc = setupSparkContext();
        SparkTransformExecutor executor = new SparkTransformExecutor();

        int i = 0;
        for (String inputPath : inputDir) {

            JavaRDD<String> rawTrainData = sc.textFile(inputPath);
            JavaRDD<Collection<Writable>> writableData = rawTrainData.map(new StringToWritablesFunction(new CSVRecordReader()));
            preprocessedData = executor.execute(writableData, seq);
            preprocessedData.cache();

            //Analyze the quality of the columns (missing values, etc), on a per column basis
            DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQuality(preprocessedSchema, preprocessedData);
            // Per-column statis summary
            DataAnalysis dataAnalyis = AnalyzeSpark.analyze(preprocessedSchema, preprocessedData);

            //Same normalization scheme for both. Normalization scheme based only on test data, however
            Triple<TransformSequence, Schema, JavaRDD<Collection<Writable>>> dataNormalized = NSLKDDUtil.normalize(preprocessedSchema, dataAnalyis, preprocessedData, executor);
            dataNormalized.getThird().cache();
            Schema normSchema = dataNormalized.getSecond();
            DataAnalysis normDataAnalysis = AnalyzeSpark.analyze(normSchema, dataNormalized.getThird());

            //Save normalized data & schema
            SparkExport.exportCSVLocal(trainTestDir.get(i), i + dataSet + "normalized", 1, ",", dataNormalized.getThird());
            FileUtils.writeStringToFile(new File(outDir, i + "normDataSchema.txt"), normSchema.toString());
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(path.NORMALIZER_FILE)))) {
                oos.writeObject(dataNormalized.getFirst());
            }

            Thread.sleep(200);
            printAnalysis(dqa, "Data quality:");
            printAnalysis(dataAnalyis, "Processed data summary:");
            printAnalysis(normDataAnalysis, "Normalized data summary:");

            //analysis and histograms
            System.out.println("Storing charts...");
            Histograms.plot(preprocessedSchema, dataAnalyis, chartDirOrig);
            Histograms.plot(normSchema, normDataAnalysis, chartDirNorm);
            System.out.println();
            preprocessedData.unpersist();
            i++;
        }

        sc.close();

    }

    public static JavaSparkContext setupSparkContext(){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        return new JavaSparkContext(sparkConf);
    }

    public static Schema defineSchema(String outDir, TransformSequence seq) throws IOException {
        //Get the sequence of transformations to make on the original data:
        Schema schema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(outDir, "preprocessedDataSchema.txt"), schema.toString());
        return schema;
    }

    public static void printAnalysis(Object analysis, String tag){
        //Wait for spark to stop its console spam before printing analysis
        System.out.println("------------------------------------------");
        System.out.println(tag);
        System.out.println(analysis);
    }



}
