package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.misc.SparkUtils;

import java.io.File;
import java.util.List;

/**Split the raw data into train and test sets, without any modifications to the data
 * Created by Alex on 13/03/2016.
 */
public class NB15SplitTrainTestRaw {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "UNSW_NB15";
    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);
    public static final String IN_DIRECTORY = PATH.IN_DIR;

    public static final String RAW_TRAIN_TEST_DIR = PATH.RAW_TRAIN_TEST_SPLIT_DIR;
    public static final String TRAIN_DIR = FilenameUtils.concat(RAW_TRAIN_TEST_DIR,"train/");
    public static final String TEST_DIR = FilenameUtils.concat(RAW_TRAIN_TEST_DIR,"test/");


    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);

        List<JavaRDD<String>> split = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN),rawData);

        String trainPath = PATH.RAW_TRAIN_PATH;
        String testPath = PATH.RAW_TEST_PATH;

        SparkExport.exportStringLocal(new File(trainPath),split.get(0),12345);
        SparkExport.exportStringLocal(new File(testPath),split.get(1),12345);
    }

}
