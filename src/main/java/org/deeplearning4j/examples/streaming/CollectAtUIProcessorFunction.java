package org.deeplearning4j.examples.streaming;

import lombok.NoArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.UIProcessor;
import org.nd4j.linalg.api.ndarray.INDArray;
import scala.Tuple3;

import java.util.List;

/**For use ONLY in foreachRDD functions, which are executed in the DRIVER
 * see: https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * using this in other circumstances is totally unsafe.
 */
@NoArgsConstructor
public class CollectAtUIProcessorFunction implements Function2<JavaRDD<Tuple3<Long, INDArray, List<Writable>>>,Time,Void> {

    @Override
    public Void call(JavaRDD<Tuple3<Long, INDArray, List<Writable>>> v1, Time time) throws Exception {
        UIProcessor uiDriver = UIProcessor.getInstance();

        List<Tuple3<Long, INDArray, List<Writable>>> list = v1.collect();

        uiDriver.addPredictions(list);

        return null;
    }
}
