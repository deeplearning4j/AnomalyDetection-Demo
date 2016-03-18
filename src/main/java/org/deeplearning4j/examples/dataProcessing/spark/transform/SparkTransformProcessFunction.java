package org.deeplearning4j.examples.dataProcessing.spark.transform;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.TransformProcess;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by Alex on 13/03/2016.
 */
@AllArgsConstructor
public class SparkTransformProcessFunction implements FlatMapFunction<Collection<Writable>,Collection<Writable>> {

    private final TransformProcess transformProcess;

    @Override
    public Iterable<Collection<Writable>> call(Collection<Writable> v1) throws Exception {
        Collection<Writable> newCollection = transformProcess.execute(v1);
        if(newCollection == null) return Collections.emptyList();   //Example was filtered out
        else return Collections.singletonList(newCollection);
    }
}