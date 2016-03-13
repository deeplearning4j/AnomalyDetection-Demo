package org.deeplearning4j.examples.misc;

import org.apache.spark.api.java.JavaRDD;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.data.split.SplitStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 7/03/2016.
 */
public class SparkUtils {

//    public static List<JavaRDD<Collection<Writable>>> splitData(SplitStrategy splitStrategy, JavaRDD<Collection<Writable>> data){
//
//        //So this is kinda ugly, but whatever.
//        if(splitStrategy instanceof RandomSplit){
//
//            RandomSplit rs = (RandomSplit)splitStrategy;
//
//            double fractionTrain = rs.getFractionTrain();
//
//            double[] splits = new double[]{fractionTrain,1.0-fractionTrain};
//
//            JavaRDD<Collection<Writable>>[] split = data.randomSplit(splits);
//            List<JavaRDD<Collection<Writable>>> list = new ArrayList<>(2);
//            Collections.addAll(list, split);
//
//            return list;
//
//        } else {
//            throw new RuntimeException("Not yet implemented");
//        }
//    }

    public static <T> List<JavaRDD<T>> splitData(SplitStrategy splitStrategy, JavaRDD<T> data){

        //So this is kinda ugly, but whatever.
        if(splitStrategy instanceof RandomSplit){

            RandomSplit rs = (RandomSplit)splitStrategy;

            double fractionTrain = rs.getFractionTrain();

            double[] splits = new double[]{fractionTrain,1.0-fractionTrain};

            JavaRDD<T>[] split = data.randomSplit(splits);
            List<JavaRDD<T>> list = new ArrayList<>(2);
            Collections.addAll(list, split);

            return list;

        } else {
            throw new RuntimeException("Not yet implemented");
        }
    }
}
