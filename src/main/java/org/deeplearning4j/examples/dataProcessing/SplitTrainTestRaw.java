package org.deeplearning4j.examples.dataProcessing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.dataProcessing.api.split.RandomSplit;
import org.deeplearning4j.examples.utils.SparkConnectFactory;
import org.deeplearning4j.examples.utils.SparkExport;
import org.deeplearning4j.examples.utils.SparkUtils;

import java.io.File;
import java.util.List;

/**
 * Split the raw data into train and test sets, without any modifications to the data
 *
 * Pass in argument that is the name of the dataset folder (e.g.) UNSW_NB15 or NSLKDD
 */
public class SplitTrainTestRaw {

    public static final long RNG_SEED = 12345;
    protected static double FRACTION_TRAIN = 0.75;


    public static void split(String dataSet) throws Exception {
        split(dataSet, SparkConnectFactory.getContext(dataSet));
    }

    public static void split(String dataSet, JavaSparkContext sc) throws Exception {
        DataPathUtil PATH = new DataPathUtil(dataSet);

        JavaRDD<String> rawData = sc.textFile(PATH.IN_DIR);

        List<JavaRDD<String>> split = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN),rawData, RNG_SEED);

        SparkExport.exportStringLocal(new File(PATH.RAW_TRAIN_FILE),split.get(0),12345);
        SparkExport.exportStringLocal(new File(PATH.RAW_TEST_FILE),split.get(1),12345);

        System.out.println("-- Done --");
    }

}