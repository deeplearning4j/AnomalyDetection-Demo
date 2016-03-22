package org.deeplearning4j.examples.models;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.lossfunctions.LossFunctions;

/**
 */

public class MLPAutoEncoderModel {

    protected int[] nIn;
    protected int[] nOut;
    protected int iterations;
    protected String activation;
    protected WeightInit weightInit;
    protected OptimizationAlgorithm optimizationAlgorithm;
    protected Updater updater;
    protected LossFunctions.LossFunction lossFunctions;
    protected double learningRate;
    protected double l2;
    double dropoutRate = 0.7;
    protected long seed;

    public MLPAutoEncoderModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit, double learningRate, double l2){
        this(nIn, nOut, iterations, activation, weightInit, OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                Updater.NESTEROVS, LossFunctions.LossFunction.MCXENT, learningRate, l2, 123);
    }


    public MLPAutoEncoderModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
                               OptimizationAlgorithm optimizationAlgorithm, Updater updater, LossFunctions.LossFunction lossFunctions,
                               double learningRate , double l2, long seed) {
        this.nIn = nIn;
        this.nOut = nOut;
        this.iterations = iterations;
        this.weightInit = weightInit;
        this.activation = activation;
        this.optimizationAlgorithm = optimizationAlgorithm;
        this.updater = updater;
        this.lossFunctions = lossFunctions;
        this.learningRate = learningRate;
        this.l2 = l2;
        this.seed = seed;
    }

    public MultiLayerConfiguration conf(){
        return new NeuralNetConfiguration.Builder()
                .iterations(iterations)
                .activation(activation)
                .dropOut(dropoutRate)
                .weightInit(weightInit)
                .learningRate(learningRate)
                .regularization(true)
                .l2(l2)
                .updater(Updater.RMSPROP)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(nIn[0]).nOut(nOut[0]).dropOut(0.3)
                        .build())
                .layer(1, new DenseLayer.Builder().nIn(nIn[1]).nOut(nOut[1]).dropOut(0.3)
                        .build())
                .layer(2, new DenseLayer.Builder().nIn(nIn[2]).nOut(nOut[2])
                        .build())
                .layer(3, new DenseLayer.Builder().nIn(nIn[3]).nOut(nOut[3])
                        .build())
                .layer(4, new DenseLayer.Builder().nIn(nIn[4]).nOut(nOut[4]).dropOut(0)
                        .build())
                .layer(5, new OutputLayer.Builder().nIn(nIn[5]).nOut(nOut[5]).dropOut(0)
                        .lossFunction(LossFunctions.LossFunction.MSE)
                        .build())
                .backprop(true).pretrain(true).build();
    }

    public MultiLayerNetwork buildModel() {
        MultiLayerConfiguration conf = conf();

        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();
        return net;
    }
}
