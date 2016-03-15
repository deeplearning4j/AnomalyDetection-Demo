package org.deeplearning4j.examples.utils;

import org.deeplearning4j.nn.conf.layers.BaseOutputLayer;
import org.deeplearning4j.nn.conf.layers.Layer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.api.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossCalculation;

/**
 * Building this util to leverage 3.9 functionality while we are tied to 3.8
 */
public class Snapshot39Util {

    // Calculate the score for each example in a DataSet individually.
    // TODO resolve issues to score single examples & add FAR calc
//    public INDArray scoreExamples(MultiLayerNetwork net, DataSet data, boolean addRegularizationTerms){
//        boolean hasMaskArray = data.hasMaskArrays();
//        if(hasMaskArray) net.setLayerMaskArrays(data.getFeaturesMaskArray(),data.getLabelsMaskArray());
//        net.feedForward(data.getFeatureMatrix(),false);
//        net.setLabels(data.getLabels());
//
//        INDArray out;
//        if( net.getOutputLayer() instanceof BaseOutputLayer){
//            BaseOutputLayer<?> ol = (BaseOutputLayer<?>) net.getOutputLayer();
//            ol.setLabels(data.getLabels());
//            double l1 = (addRegularizationTerms ? net.calcL1() : 0.0);
//            double l2 = (addRegularizationTerms ? net.calcL2() : 0.0);
//            out = ol.computeScoreForExamples(mln, l1,l2);
//        } else {
//            throw new UnsupportedOperationException("Cannot calculate score wrt labels without an OutputLayer");
//        }
//        if(hasMaskArray) net.clearLayerMaskArrays();
//        return out;
//    }
//
//    public INDArray computeScoreForExamples(BaseOutputLayer layer, double fullNetworkL1, double fullNetworkL2){
//        if( input == null || labels == null )
//            throw new IllegalStateException("Cannot calculate score without input and labels");
//        INDArray preOut = layer.preOutput2d(false);
//        INDArray output = Nd4j.getExecutioner().execAndReturn(Nd4j.getOpFactory().createTransform(layer.getActivationFunction(), preOut.dup()));
//
//        return LossCalculation.builder()
//                .l1(fullNetworkL1).l2(fullNetworkL2)
//                .labels(layer.getLabels2d()).z(output)
//                .preOut(preOut).activationFn(layer.getActivationFunction())
//                .lossFunction(layer.getLossFunction())
//                .useRegularization(layer.)
//                .mask(layer.maskArray).build().scoreExamples();
//    }

}
