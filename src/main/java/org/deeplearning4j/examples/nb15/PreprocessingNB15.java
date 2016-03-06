package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.TransformSequence;
import org.deeplearning4j.examples.data.analysis.AnalyzeSpark;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.dataquality.QualityAnalyzeSpark;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.filter.FilterInvalidValues;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.data.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceInvalidWithInteger;
import org.deeplearning4j.examples.data.transform.string.RemoveWhiteSpaceTransform;
import org.deeplearning4j.examples.data.transform.string.ReplaceEmptyStringTransform;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class PreprocessingNB15 {

    public static void main(String[] args) throws Exception {

        //Get the initial schema
        Schema csvSchema = NB15Util.getNB15CsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = new TransformSequence.Builder(csvSchema)
                .transform(new RemoveWhiteSpaceTransform("attack category"))
                .transform(new ReplaceEmptyStringTransform("attack category", "none"))  //Replace empty strings in "attack category"
                .filter(new FilterInvalidValues("source port", "destination port")) //Remove examples/rows that have invalid values for these rows
                .transform(new ReplaceEmptyIntegerWithValueTransform("count flow http methods",0))
                .transform(new ReplaceInvalidWithInteger("count ftp commands",0))
                .build();

        Schema finalSchema = seq.getFinalSchema(csvSchema);


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        String dataDir = "C:/DL4J/Git/AnomalyDetection-Demo/src/main/resources/";   //Subset of data
        String dataDir = "C:/Data/UNSW_NB15/CSV/";
        JavaRDD<String> rawData = sc.textFile(dataDir);

//        String inputName = "csv_50_records.txt";
//        String basePath = new ClassPathResource(inputName).getFile().getAbsolutePath();
//        JavaRDD<String> rawData = sc.textFile(basePath);


        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));


        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> processedData = executor.execute(data, seq);
        processedData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQuality(finalSchema, processedData);

        //Do analysis, on a per-column basis
//        DataAnalysis da = AnalyzeSpark.analyze(finalSchema, processedData);
        List<Writable> invalidIsFtpLogin = QualityAnalyzeSpark.sampleInvalidColumns(100,"is ftp login",finalSchema,processedData);
//        List<Writable> invalidSourceTCPBaseSequenceNum = QualityAnalyzeSpark.sampleInvalidColumns(100,"source TCP base sequence num",finalSchema,processedData);
//        List<Writable> invalidDestTCPBaseSequenceNum = QualityAnalyzeSpark.sampleInvalidColumns(100,"dest TCP base sequence num",finalSchema,processedData);
        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");
        System.out.println("Data quality:");
        System.out.println(dqa);

        System.out.println("------------------------------------------");

//        System.out.println(da);

        //TODO: analysis and histograms

        System.out.println("Invalid is ftp login data:");
        System.out.println(invalidIsFtpLogin);

        System.out.println();
    }

}
