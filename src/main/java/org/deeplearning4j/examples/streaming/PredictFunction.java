package org.deeplearning4j.examples.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Alex on 10/03/2016.
 */
public class PredictFunction implements FlatMapFunction<Iterator<Tuple2<Long,INDArray>>,Tuple2<Long,INDArray>> {
    public static final int DEFAULT_BATCH_SIZE = 64;

    private final Broadcast<String> config;
    private final Broadcast<INDArray> params;
    private final int batchSize;

    private MultiLayerNetwork net;

    public PredictFunction(Broadcast<String> config, Broadcast<INDArray> params) {
        this(config,params,DEFAULT_BATCH_SIZE);
    }

    public PredictFunction(Broadcast<String> config, Broadcast<INDArray> params, int batchSize) {
        this.config = config;
        this.params = params;
        this.batchSize = batchSize;
    }

    @Override
    public Iterable<Tuple2<Long,INDArray>> call(Iterator<Tuple2<Long, INDArray>> iter) throws Exception {

        if(net == null){
            String conf = config.getValue();
            INDArray p = params.getValue();
            MultiLayerConfiguration mlc = MultiLayerConfiguration.fromJson(conf);

            net = new MultiLayerNetwork(mlc);
            net.init();
            net.setParams(p);
        }

        List<Tuple2<Long,INDArray>> predictions = new ArrayList<>();
        List<INDArray> collected = new ArrayList<>(batchSize);
        List<Long> exampleNums = new ArrayList<>(batchSize);
        while(iter.hasNext()) {
            for (int i = 0; i < batchSize && iter.hasNext(); i++) {
                Tuple2<Long,INDArray> t2 = iter.next();
                collected.add(t2._2());
                exampleNums.add(t2._1());
            }

            INDArray inData = Nd4j.vstack(collected.toArray(new INDArray[collected.size()]));
            INDArray out = net.output(inData,false);
            for(int i=0; i<collected.size(); i++ ){
                INDArray outi = out.getRow(i).dup();
                Long keyi = exampleNums.get(i);
                predictions.add(new Tuple2<>(keyi,outi));
            }
            
            collected.clear();
            exampleNums.clear();
        }

        return predictions;
    }
}
