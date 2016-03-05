package org.deeplearning4j.examples.data.executor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Transform;
import org.deeplearning4j.examples.data.TransformationSequence;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.data.split.SplitStrategy;
import org.deeplearning4j.examples.data.transform.spark.SparkTransformFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SparkTransformExecutor {


    public JavaRDD<Collection<Writable>> execute(JavaRDD<Collection<Writable>> inputWritables, TransformationSequence sequence ){

        JavaRDD<Collection<Writable>> currentWritables = inputWritables;

        List<Transform> list = sequence.getTransformationList();

        for(Transform t : list ){

            Function<Collection<Writable>,Collection<Writable>> function = new SparkTransformFunction(t);
            currentWritables = currentWritables.map(function);
        }

        return currentWritables;
    }


    public List<JavaRDD<Collection<Writable>>> splitData(SplitStrategy splitStrategy, JavaRDD<Collection<Writable>> data){

        //So this is kinda ugly, but whatever.
        if(splitStrategy instanceof RandomSplit){

            RandomSplit rs = (RandomSplit)splitStrategy;

            double fractionTrain = rs.getFractionTrain();

            double[] splits = new double[]{fractionTrain,1.0-fractionTrain};

            JavaRDD<Collection<Writable>>[] split = data.randomSplit(splits);
            List<JavaRDD<Collection<Writable>>> list = new ArrayList<>(2);
            Collections.addAll(list, split);

            return list;

        } else {
            throw new RuntimeException("Not yet implemented");
        }

    }


}
