package org.deeplearning4j.examples.streaming;

import lombok.NoArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.UIDriver;
import org.nd4j.linalg.api.ndarray.INDArray;
import scala.Tuple3;

import java.util.Collection;
import java.util.List;

/**For use ONLY in foreachRDD functions, which are executed in the DRIVER
 * see: https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * using this in other circumstances is totally unsafe.
 * Created by Alex on 14/03/2016.
 */
@NoArgsConstructor
public class CollectAtUIDriverFunction implements Function2<JavaRDD<Tuple3<Long, INDArray, List<Writable>>>,Time,Void> {

    @Override
    public Void call(JavaRDD<Tuple3<Long, INDArray, List<Writable>>> v1, Time time) throws Exception {
        UIDriver uiDriver = UIDriver.getInstance();

        List<Tuple3<Long, INDArray, List<Writable>>> list = v1.collect();

//        System.out.println("*** " + time + " - " + list);
        uiDriver.addPredictions(list);

        return null;
    }
}
