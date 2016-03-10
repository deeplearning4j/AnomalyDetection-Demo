package org.deeplearning4j.examples.iscx;

import com.jcraft.jsch.MAC;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.dataquality.QualityAnalyzeSpark;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;

import java.util.Collection;

/**
 * Created by Alex on 4/03/2016.
 */
public class AnalysisISCX {

    public static final boolean win = System.getProperty("os.name").toLowerCase().contains("win");

    public static final String WIN_DIR = "C:/Data/ISCX/CSV/";
    public static final String MAC_DIR = FilenameUtils.concat(System.getProperty("user.home"), "data/NIDS/ISCX/convert/");

    public static final String DATA_DIR = (win ? WIN_DIR : MAC_DIR);


    public static void main(String[] args) throws Exception {
        Schema csvSchema = ISCXUtil.getCsvSchema();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("ISCX");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(DATA_DIR);

        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));


        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQuality(csvSchema,data);
        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");
        System.out.println("Data quality:");
        System.out.println(dqa);
    }
}
