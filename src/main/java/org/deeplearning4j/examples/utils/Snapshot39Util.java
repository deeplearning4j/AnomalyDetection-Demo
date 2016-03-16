package org.deeplearning4j.examples.utils;

import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.conf.layers.BaseOutputLayer;

import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.api.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossCalculation;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import scala.tools.reflect.Eval;

import java.util.List;

/**
 * Building this util to leverage 3.9 functionality while we are tied to 3.8
 */
public class Snapshot39Util {

    public Snapshot39Util(){}

    public double scoreExample(MultiLayerNetwork net, DataSet data, boolean addRegularizationTerms, LossFunctions.LossFunction lossFunction){
        List<INDArray> activations = net.feedForward(data.getFeatureMatrix(),false);
        net.setLabels(data.getLabels());

        double out;
        net.setLabels(data.getLabels());
        double l1 = (addRegularizationTerms ? net.calcL1() : 0.0);
        double l2 = (addRegularizationTerms ? net.calcL2() : 0.0);
        out = computeScoreForExamples(net, l1,l2, lossFunction, activations);
        return out;
    }

    //TODO - correct score?
    public double computeScoreForExamples(MultiLayerNetwork net, double fullNetworkL1, double fullNetworkL2, LossFunctions.LossFunction lossFunction, List<INDArray> activations){
        org.deeplearning4j.nn.layers.BaseOutputLayer layer = (org.deeplearning4j.nn.layers.BaseOutputLayer) net.getOutputLayer();
        INDArray preOut = layer.preOutput(activations.get(net.getnLayers()-1), false);

        INDArray output = Nd4j.getExecutioner().execAndReturn(Nd4j.getOpFactory().createTransform("softmax", preOut.dup()));

        return LossCalculation.builder()
                .l1(fullNetworkL1).l2(fullNetworkL2)
                .labels(net.getLabels()).z(output)
                .preOut(preOut).activationFn("softmax")
                .lossFunction(lossFunction)
                .useRegularization(net.conf().isUseRegularization())
                .build().score();
    }




}
