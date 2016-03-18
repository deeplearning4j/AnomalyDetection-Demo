package org.deeplearning4j.examples.dataProcessing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.TransformProcess;
import org.deeplearning4j.examples.dataProcessing.spark.AnalyzeSpark;
import org.deeplearning4j.examples.dataProcessing.spark.SparkTransformExecutor;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.spark.misc.StringToWritablesFunction;
import org.deeplearning4j.examples.datasets.iscx.ISCXUtil;
import org.deeplearning4j.examples.utils.SparkExport;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;

import java.io.File;
import java.util.Collection;

/**Preprocessing - playing around with sequences
 */
public class PreprocessingSequence extends PreprocessingPreSplit{


    public static void main(String[] args) throws Exception {
        setup(args[0]);
        JavaSparkContext sc = setupSparkContext();

        SparkTransformExecutor executor = new SparkTransformExecutor();
        Triple<TransformProcess, Schema, JavaRDD<Collection<Collection<Writable>>>> dataNormalized = null;


        int i = 0;
        for (String inputPath : inputDir) {

            JavaRDD<String> rawData = sc.textFile(inputPath);
            JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));
            JavaRDD<Collection<Collection<Writable>>> sequenceData = executor.executeToSequence(data, sequence);
            sequenceData.cache();

            runSequenceAnalysis(preprocessedSchema, sequenceData);

            //Same normalization scheme for both. Normalization scheme based only on test data, however
            switch (dataSet) {
                case "UNSW_NB15":
                    dataNormalized = NB15Util.normalizeSequence(preprocessedSchema, dataAnalyis, sequenceData, executor);
                    break;
                case "NSLKDD":
                    dataNormalized = NSLKDDUtil.normalizeSequence(preprocessedSchema, dataAnalyis, sequenceData, executor);
                    break;
                case "ISCX":
                    dataNormalized = ISCXUtil.normalizeSequence(preprocessedSchema, dataAnalyis, sequenceData, executor);
                    break;
            }
            dataNormalized.getThird().cache();
            normSchema = dataNormalized.getSecond();
            normDataAnalysis = AnalyzeSpark.analyzeSequence(normSchema, dataNormalized.getThird());

            //Save normalized data & schema
            SparkExport.exportCSVSequenceLocal(new File(trainTestDir.get(i)), dataNormalized.getThird());
            if (i == 0) {
                saveNormSchemaTransform(dataNormalized.getFirst());
            }


//        List<Writable> samplesDirection = AnalyzeSpark.sampleFromColumn(100,"direction",preprocessedSchema,processedData);
//        List<Writable> samplesUnique = AnalyzeSpark.getUnique("source TCP flags",preprocessedSchema,processedData);
            i++;
            sequenceData.unpersist();
            dataNormalized.getThird().unpersist();
        }
        printAndStoreAnalysis();
        sc.close();
    }



    public static void runSequenceAnalysis(Schema schema, JavaRDD<Collection<Collection<Writable>>> data){
        //Analyze the quality of the columns (missing values, etc), on a per column basis
        dqa = AnalyzeSpark.analyzeQualitySequence(schema, data);
        //Do analysis, on a per-column basis
        dataAnalyis = AnalyzeSpark.analyzeSequence(schema, data);
    }


}
