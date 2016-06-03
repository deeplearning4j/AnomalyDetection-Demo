package org.deeplearning4j.examples.datasets.iscx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.utils.DataPathUtil;
import io.skymind.echidna.api.dataquality.DataQualityAnalysis;
import io.skymind.echidna.api.schema.Schema;
import io.skymind.echidna.spark.AnalyzeSpark;
import io.skymind.echidna.spark.misc.StringToWritablesFunction;

import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class AnalysisISCX {

    protected static String dataSet = "ISCX";
    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);
    public static final String IN_DIRECTORY = PATH.IN_DIR;

    public static void main(String[] args) throws Exception {
        Schema csvSchema = ISCXUtil.getCsvSchema();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("ISCX");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);

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
