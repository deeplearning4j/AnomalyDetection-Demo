package org.deeplearning4j.examples.datasets.nb15;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.util.ClassPathResource;
import org.datavec.api.writable.Writable;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.quality.DataQualityAnalysis;
import org.datavec.spark.transform.AnalyzeSpark;
import org.datavec.spark.transform.misc.StringToWritablesFunction;

import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class AnalysisNB15 {

    public static void main(String[] args) throws Exception {
        Schema csvSchema = NB15Util.getCsvSchema();

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

        JavaRDD<List<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));


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
