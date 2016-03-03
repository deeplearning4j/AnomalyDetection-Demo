package org.deeplearning4j.examples.Models;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.distribution.UniformDistribution;
import org.deeplearning4j.nn.conf.layers.GravesLSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.lossfunctions.LossFunctions;

/**
 * Resource: http://sacj.cs.uct.ac.za/index.php/sacj/article/viewFile/248/150
 */
public class BasicRNNModel {

    protected int nIn;
    protected int nOut;
    protected int lstmLayerSize;
    protected int truncatedBPTTLength;
    private int iterations = 1;
    private long seed = 123;

    public BasicRNNModel(int nIn, int nOut, int lstmLayerSize, int truncatedBPTTLength) {
        this(nIn, nOut, lstmLayerSize, truncatedBPTTLength, 1, 123);
    }


    public BasicRNNModel(int nIn, int nOut, int lstmLayerSize, int truncatedBPTTLength, int iterations) {
        this(nIn, nOut, lstmLayerSize, truncatedBPTTLength, iterations, 123);
    }

    public BasicRNNModel(int nIn, int nOut, int lstmLayerSize, int truncatedBPTTLength, int iterations, long seed) {
        this.nIn = nIn;
        this.nOut = nOut;
        this.lstmLayerSize = lstmLayerSize;
        this.truncatedBPTTLength = truncatedBPTTLength;
        this.iterations = iterations;
        this.seed = seed;
    }

    protected MultiLayerNetwork buildModel() {
        System.out.println("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .iterations(iterations)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
                .learningRate(0.1)
                .rmsDecay(0.95)
                .seed(seed)
                .regularization(true)
                .l2(0.001)
                .list()
                .layer(0, new GravesLSTM.Builder().nIn(nIn).nOut(lstmLayerSize)
                        .updater(Updater.RMSPROP)
                        .activation("tanh").weightInit(WeightInit.DISTRIBUTION)
                        .dist(new UniformDistribution(-0.08, 0.08)).build())
                .layer(1, new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
                        .updater(Updater.RMSPROP)
                        .activation("tanh").weightInit(WeightInit.DISTRIBUTION)
                        .dist(new UniformDistribution(-0.08, 0.08)).build())
                .layer(2, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT).activation("softmax")        //MCXENT + softmax for classification
                        .updater(Updater.RMSPROP)
                        .nIn(lstmLayerSize).nOut(nOut).weightInit(WeightInit.DISTRIBUTION)
                        .dist(new UniformDistribution(-0.08, 0.08)).build())
                .pretrain(false).backprop(true)
                .backpropType(BackpropType.TruncatedBPTT).tBPTTForwardLength(truncatedBPTTLength).tBPTTBackwardLength(truncatedBPTTLength)
                .build();

        MultiLayerNetwork network = new MultiLayerNetwork(conf);
        network.init();
        return network;

    }


}
