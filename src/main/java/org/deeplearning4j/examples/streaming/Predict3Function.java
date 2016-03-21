package org.deeplearning4j.examples.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.canova.api.writable.Writable;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Alex on 10/03/2016.
 */
public class Predict3Function implements FlatMapFunction<Iterator<Tuple3<Long,INDArray,List<Writable>>>,
        Tuple3<Long,INDArray,List<Writable>>> {
    public static final int DEFAULT_BATCH_SIZE = 64;

    private final Broadcast<String> config;
    private final Broadcast<INDArray> params;
    private final int batchSize;

    private MultiLayerNetwork net;

    public Predict3Function(Broadcast<String> config, Broadcast<INDArray> params) {
        this(config,params,DEFAULT_BATCH_SIZE);
    }

    public Predict3Function(Broadcast<String> config, Broadcast<INDArray> params, int batchSize) {
        this.config = config;
        this.params = params;
        this.batchSize = batchSize;
    }

    @Override
    public Iterable<Tuple3<Long,INDArray,List<Writable>>> call(Iterator<Tuple3<Long, INDArray, List<Writable>>> iter) throws Exception {

        if(net == null){
            String conf = config.getValue();
            INDArray p = params.getValue();
            MultiLayerConfiguration mlc = MultiLayerConfiguration.fromJson(conf);

            net = new MultiLayerNetwork(mlc);
            net.init();
            net.setParams(p);
        }

        List<Tuple3<Long,INDArray,List<Writable>>> predictions = new ArrayList<>();
        List<INDArray> collected = new ArrayList<>(batchSize);
        List<Long> exampleNums = new ArrayList<>(batchSize);
        List<List<Writable>> writables = new ArrayList<>(batchSize);
        while(iter.hasNext()) {
            for (int i = 0; i < batchSize && iter.hasNext(); i++) {
                Tuple3<Long,INDArray,List<Writable>> t2 = iter.next();
                collected.add(t2._2());
                exampleNums.add(t2._1());
                writables.add(t2._3());
            }

            INDArray inData = Nd4j.vstack(collected.toArray(new INDArray[collected.size()]));
            INDArray out = net.output(inData,false);
            for(int i=0; i<collected.size(); i++ ){
                INDArray outi = out.getRow(i).dup();
                Long keyi = exampleNums.get(i);
                List<Writable> cw = writables.get(i);
                predictions.add(new Tuple3<>(keyi,outi,cw));
            }
            
            collected.clear();
            exampleNums.clear();
        }

        return predictions;
    }
}
