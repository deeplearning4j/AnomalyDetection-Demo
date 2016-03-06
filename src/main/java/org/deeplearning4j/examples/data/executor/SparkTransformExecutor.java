package org.deeplearning4j.examples.data.executor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.*;
import org.deeplearning4j.examples.data.analysis.sparkfunctions.SelectColumnFunction;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.spark.FilterWritablesBySchemaFunction;
import org.deeplearning4j.examples.data.spark.SparkFilterFunction;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.data.split.SplitStrategy;
import org.deeplearning4j.examples.data.spark.SparkTransformFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SparkTransformExecutor {


    public JavaRDD<Collection<Writable>> execute(JavaRDD<Collection<Writable>> inputWritables, TransformSequence sequence ){

        JavaRDD<Collection<Writable>> currentWritables = inputWritables;

        List<DataAction> list = sequence.getActionList();

        for(DataAction d : list ){

            if(d.getTransform() != null) {
                Transform t = d.getTransform();
                Function<Collection<Writable>, Collection<Writable>> function = new SparkTransformFunction(t);
                currentWritables = currentWritables.map(function);
            } else {
                //Filter
                Filter f = d.getFilter();
                currentWritables = currentWritables.filter(new SparkFilterFunction(f));
            }
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
