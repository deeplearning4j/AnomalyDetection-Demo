package org.deeplearning4j.examples.dataProcessing.spark;

import org.apache.commons.math3.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.DataAction;
import org.deeplearning4j.examples.dataProcessing.api.filter.Filter;
import org.deeplearning4j.examples.dataProcessing.api.Transform;
import org.deeplearning4j.examples.dataProcessing.api.TransformProcess;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.schema.SequenceSchema;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertToSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceSplit;
import org.deeplearning4j.examples.dataProcessing.spark.filter.SparkFilterFunction;
import org.deeplearning4j.examples.dataProcessing.spark.reduce.MapToPairForReducerFunction;
import org.deeplearning4j.examples.dataProcessing.spark.reduce.ReducerFunction;
import org.deeplearning4j.examples.dataProcessing.spark.sequence.SparkGroupToSequenceFunction;
import org.deeplearning4j.examples.dataProcessing.spark.sequence.SparkMapToPairByColumnFunction;
import org.deeplearning4j.examples.dataProcessing.spark.sequence.SparkSequenceFilterFunction;
import org.deeplearning4j.examples.dataProcessing.spark.sequence.SparkSequenceTransformFunction;
import org.deeplearning4j.examples.dataProcessing.spark.transform.SequenceSplitFunction;
import org.deeplearning4j.examples.dataProcessing.spark.transform.SparkTransformFunction;

import java.util.Collection;
import java.util.List;

public class SparkTransformExecutor {


    public JavaRDD<Collection<Writable>> execute(JavaRDD<Collection<Writable>> inputWritables, TransformProcess sequence ) {
        if(sequence.getFinalSchema() instanceof SequenceSchema){
            throw new IllegalStateException("Cannot return sequence data with this method");
        }

        return execute(inputWritables,null,sequence).getFirst();
//        return inputWritables.flatMap(new SparkTransformProcessFunction(sequence));    //Only works if no toSequence or FromSequence ops are in the TransformSequenc...
    }

    public JavaRDD<Collection<Collection<Writable>>> executeToSequence(JavaRDD<Collection<Writable>> inputWritables, TransformProcess sequence ) {
        if(!(sequence.getFinalSchema() instanceof SequenceSchema)){
            throw new IllegalStateException("Cannot return non-sequence data with this method");
        }

        return execute(inputWritables,null,sequence).getSecond();
    }

    public JavaRDD<Collection<Writable>> executeSequenceToSeparate(JavaRDD<Collection<Collection<Writable>>> inputSequence, TransformProcess sequence ) {
        if(sequence.getFinalSchema() instanceof SequenceSchema){
            throw new IllegalStateException("Cannot return sequence data with this method");
        }

        return execute(null,inputSequence,sequence).getFirst();
    }

    public JavaRDD<Collection<Collection<Writable>>> executeSequenceToSequence(JavaRDD<Collection<Collection<Writable>>> inputSequence, TransformProcess sequence ) {
        if(!(sequence.getFinalSchema() instanceof SequenceSchema)){
            throw new IllegalStateException("Cannot return non-sequence data with this method");
        }

        return execute(null,inputSequence,sequence).getSecond();
    }


    private Pair<JavaRDD<Collection<Writable>>,JavaRDD<Collection<Collection<Writable>>>>
        execute(JavaRDD<Collection<Writable>> inputWritables, JavaRDD<Collection<Collection<Writable>>> inputSequence,
                TransformProcess sequence ){
        JavaRDD<Collection<Writable>> currentWritables = inputWritables;
        JavaRDD<Collection<Collection<Writable>>> currentSequence = inputSequence;

        List<DataAction> list = sequence.getActionList();

        for(DataAction d : list ){

            if(d.getTransform() != null) {
                Transform t = d.getTransform();
                if(currentWritables != null){
                    Function<Collection<Writable>, Collection<Writable>> function = new SparkTransformFunction(t);
                    currentWritables = currentWritables.map(function);
                } else {
                    Function<Collection<Collection<Writable>>, Collection<Collection<Writable>>> function =
                            new SparkSequenceTransformFunction(t);
                    currentSequence = currentSequence.map(function);
                }
            } else if(d.getFilter() != null ){
                //Filter
                Filter f = d.getFilter();
                if(currentWritables != null){
                    currentWritables = currentWritables.filter(new SparkFilterFunction(f));
                } else {
                    currentSequence = currentSequence.filter(new SparkSequenceFilterFunction(f));
                }

            } else if(d.getConvertToSequence() != null) {
                //Convert to a sequence...
                ConvertToSequence cts = d.getConvertToSequence();

                //First: convert to PairRDD
                Schema schema = cts.getInputSchema();
                int colIdx = schema.getIndexOfColumn(cts.getKeyColumn());
                JavaPairRDD<Writable, Collection<Writable>> withKey = currentWritables.mapToPair(new SparkMapToPairByColumnFunction(colIdx));
                JavaPairRDD<Writable, Iterable<Collection<Writable>>> grouped = withKey.groupByKey();

                //Now: convert to a sequence...
                currentSequence = grouped.map(new SparkGroupToSequenceFunction(cts.getComparator()));
                currentWritables = null;
            } else if(d.getConvertFromSequence() != null ) {
                //Convert from sequence...
                ConvertFromSequence cfs = d.getConvertFromSequence();

                throw new RuntimeException("Not yet implemented");
            } else if(d.getSequenceSplit() != null ) {
                SequenceSplit sequenceSplit = d.getSequenceSplit();
                currentSequence = currentSequence.flatMap(new SequenceSplitFunction(sequenceSplit));
            } else if(d.getReducer() != null){
                IReducer reducer = d.getReducer();

                if(currentWritables == null) throw new IllegalStateException("Error during execution: current writables are null. "
                    + "Trying to execute a reduce operation on a sequence?");
                JavaPairRDD<String,Collection<Writable>> pair = currentWritables.mapToPair(new MapToPairForReducerFunction(reducer));

                currentWritables = pair.groupByKey().map(new ReducerFunction(reducer));
            } else {
                throw new RuntimeException("Unknown/not implemented action: d");
            }
        }

        return new Pair<>(currentWritables,currentSequence);
    }
}
