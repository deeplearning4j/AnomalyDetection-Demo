package org.deeplearning4j.examples.data.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.data.api.split.RandomSplit;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.misc.SparkUtils;

import java.io.File;
import java.util.List;

/**Split the raw data into train and test sets, without any modifications to the data
 * Created by Alex on 13/03/2016.
 */
public class SplitTrainTestRaw {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet;

    public static void main(String[] args) throws Exception {
        dataSet = args[0];
        DataPathUtil PATH = new DataPathUtil(dataSet);
        String IN_DIRECTORY = PATH.IN_DIR;

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("TestSplit");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);

        List<JavaRDD<String>> split = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN),rawData);

        SparkExport.exportStringLocal(new File(PATH.RAW_TRAIN_PATH),split.get(0),12345);
        SparkExport.exportStringLocal(new File(PATH.RAW_TEST_PATH),split.get(1),12345);
    }

}
