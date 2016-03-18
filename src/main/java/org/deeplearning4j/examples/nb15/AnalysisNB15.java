package org.deeplearning4j.examples.nb15;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.schema.Schema;
import org.deeplearning4j.examples.data.api.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.spark.AnalyzeSpark;
import org.deeplearning4j.examples.data.spark.misc.StringToWritablesFunction;

import java.util.Collection;

/**
 * Created by Alex on 4/03/2016.
 */
public class AnalysisNB15 {

    public static void main(String[] args) throws Exception {
        Schema csvSchema = NB15Util.getNB15CsvSchema();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        String dataDir = "C:/DL4J/Git/AnomalyDetection-Demo/src/main/resources/";
//        String dataDir = "C:/Data/UNSW_NB15/CSV/";
//        JavaRDD<String> rawData = sc.textFile(dataDir);

        String inputName = "csv_50_records.txt";
        String basePath = new ClassPathResource(inputName).getFile().getAbsolutePath();
        JavaRDD<String> rawData = sc.textFile(basePath);

        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));


        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = AnalyzeSpark.analyzeQuality(csvSchema,data);
        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");
        System.out.println("Data quality:");
        System.out.println(dqa);
    }
}
