package org.deeplearning4j.examples;
import io.skymind.echidna.api.split.RandomSplit;
import io.skymind.echidna.spark.utils.SparkExport;
import io.skymind.echidna.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.examples.utils.SparkConnectFactory;

import java.io.File;
import java.util.List;

/**
 * Split the raw data into train and test sets, without any modifications to the data
 *
 * Pass in argument that is the name of the dataset folder
 */
public class SplitTrainTestRaw {

    public static final long RNG_SEED = 12345;
    protected static double FRACTION_TRAIN = 0.75;



    public static void split(String inputDir,  String trainFile, String testFile) throws Exception {
        split(inputDir,  trainFile, testFile, SparkConnectFactory.getContext("split"));
    }

    public static void split(String inputDir, String trainFile, String testFile, JavaSparkContext sc) throws Exception {

        JavaRDD<String> rawData = sc.textFile(inputDir);

        List<JavaRDD<String>> split = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN),rawData, RNG_SEED);

        SparkExport.exportStringLocal(new File(trainFile),split.get(0),12345);
        SparkExport.exportStringLocal(new File(testFile),split.get(1),12345);

        System.out.println("-- Done --");
    }

}