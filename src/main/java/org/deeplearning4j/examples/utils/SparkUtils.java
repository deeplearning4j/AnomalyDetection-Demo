package org.deeplearning4j.examples.utils;

import org.apache.spark.api.java.JavaRDD;
import org.deeplearning4j.examples.dataProcessing.api.split.RandomSplit;
import org.deeplearning4j.examples.dataProcessing.api.split.SplitStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 7/03/2016.
 */
public class SparkUtils {

    public static <T> List<JavaRDD<T>> splitData(SplitStrategy splitStrategy, JavaRDD<T> data, long seed){

        //So this is kinda ugly, but whatever.
        if(splitStrategy instanceof RandomSplit){

            RandomSplit rs = (RandomSplit)splitStrategy;

            double fractionTrain = rs.getFractionTrain();

            double[] splits = new double[]{fractionTrain,1.0-fractionTrain};

            JavaRDD<T>[] split = data.randomSplit(splits, seed);
            List<JavaRDD<T>> list = new ArrayList<>(2);
            Collections.addAll(list, split);

            return list;

        } else {
            throw new RuntimeException("Not yet implemented");
        }
    }
}
