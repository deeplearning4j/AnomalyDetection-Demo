package org.deeplearning4j.examples.dataProcessing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.datasets.iscx.ISCXUtil;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;
import org.deeplearning4j.preprocessing.api.TransformProcess;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.preprocessing.api.split.RandomSplit;
import org.deeplearning4j.preprocessing.spark.AnalyzeSpark;
import org.deeplearning4j.preprocessing.spark.SparkTransformExecutor;
import org.deeplearning4j.preprocessing.spark.misc.StringToWritablesFunction;
import org.deeplearning4j.examples.utils.SparkConnectFactory;
import org.deeplearning4j.preprocessing.spark.utils.SparkExport;
import org.deeplearning4j.preprocessing.spark.utils.SparkUtils;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**Preprocessing - sequences.
 * Here: we can't split on the raw data. Instead, we split AFTER creating sequences
 */
public class PreprocessingSequence extends PreprocessingPreSplit {


    public static void main(String... args) throws Exception {
        setup(args[0], true);
        JavaSparkContext sc = SparkConnectFactory.getContext(dataSet);
        trainTestDir = Arrays.asList(path.PRE_SEQ_TRAIN_DATA_DIR, path.PRE_SEQ_TEST_DATA_DIR);

        SparkTransformExecutor executor = new SparkTransformExecutor();
        Triple<TransformProcess, Schema, JavaRDD<List<List<Writable>>>> dataNormalized = null;

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);
        JavaRDD<List<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));
        JavaRDD<List<List<Writable>>> sequenceData = executor.executeToSequence(data, transformProcess);
        sequenceData.cache();

        //Split the data:
        List<JavaRDD<List<List<Writable>>>> trainAndTest = SparkUtils.splitData(new RandomSplit(0.75),sequenceData, RNG_SEED);
        JavaRDD<List<List<Writable>>> trainData = trainAndTest.get(0);
        JavaRDD<List<List<Writable>>> testData = trainAndTest.get(1);

        runSequenceAnalysis(preprocessedSchema, trainData);

        //Same normalization scheme for both. Normalization scheme based only on test data, however
        for(int i=0; i<=1; i++ ) {
            JavaRDD<List<List<Writable>>> toNormalize = (i == 0 ? trainData : testData);

            switch (dataSet) {
                case "UNSW_NB15":
                    dataNormalized = NB15Util.normalizeSequence(preprocessedSchema, dataAnalysis, toNormalize, executor);
                    break;
                case "NSLKDD":
                    dataNormalized = NSLKDDUtil.normalizeSequence(preprocessedSchema, dataAnalysis, toNormalize, executor);
                    break;
                case "ISCX":
                    dataNormalized = ISCXUtil.normalizeSequence(preprocessedSchema, dataAnalysis, toNormalize, executor);
                    break;
                default:
                    throw new RuntimeException("Unknown data set: " + dataSet);
            }

            //Save normalized data & schema
            SparkExport.exportCSVSequenceLocal(new File(trainTestDir.get(i)), dataNormalized.getThird(), RNG_SEED);
            if (i == 0) {
                normSchema = dataNormalized.getSecond();
                normDataAnalysis = AnalyzeSpark.analyzeSequence(normSchema, dataNormalized.getThird());
                saveNormSchemaTransform(dataNormalized.getFirst());
            }
        }


//        List<Writable> samplesDirection = AnalyzeSpark.sampleFromColumn(100,"direction",preprocessedSchema,processedData);
//        List<Writable> samplesUnique = AnalyzeSpark.getUnique("source TCP flags",preprocessedSchema,processedData);
        sequenceData.unpersist();
        dataNormalized.getThird().unpersist();
        printAndStoreAnalysis();
        sc.close();
    }



    public static void runSequenceAnalysis(Schema schema, JavaRDD<List<List<Writable>>> data){
        //Analyze the quality of the columns (missing values, etc), on a per column basis
        dqa = AnalyzeSpark.analyzeQualitySequence(schema, data);
        //Do analysis, on a per-column basis
        dataAnalysis = AnalyzeSpark.analyzeSequence(schema, data);
    }


}
