package org.deeplearning4j.examples.utils;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.deeplearning4j.berkeley.Pair;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;


/**
 */
public class ScoreMapFunction implements Function<DataSet,Pair<Double, DataSet>> {

    private final Broadcast<String> jsonConf;
    private final Broadcast<INDArray> params;
    private MultiLayerNetwork net = null;

    public ScoreMapFunction(Broadcast<String> jsonConf, Broadcast<INDArray> params) {
        this.jsonConf = jsonConf;
        this.params = params;
    }

    @Override
    public Pair<Double, DataSet> call(DataSet ds) throws Exception {
        if(net == null){
            MultiLayerConfiguration conf = MultiLayerConfiguration.fromJson(jsonConf.getValue());
            net = new MultiLayerNetwork(conf);
            net.init();
            net.setParams(params.getValue());
        }
        INDArray arr = ds.getFeatureMatrix();

        double score = net.score(new DataSet(arr, arr), false);

        return new Pair<>(score, ds);
    }
}
