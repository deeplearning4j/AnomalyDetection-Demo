package org.deeplearning4j.examples.models;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.variational.GaussianReconstructionDistribution;
import org.deeplearning4j.nn.conf.layers.variational.VariationalAutoencoder;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.lossfunctions.LossFunctions;

/**
 */

public class VariationalAutoEncoderModel {

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
    double dropoutRate = 0.0;
    protected long seed;

    public VariationalAutoEncoderModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit, double learningRate, double l2){
        this(nIn, nOut, iterations, activation, weightInit, OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                Updater.ADAM, LossFunctions.LossFunction.RECONSTRUCTION_CROSSENTROPY, learningRate, l2, 123);
    }


    public VariationalAutoEncoderModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
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
            .seed(seed)
            .learningRate(learningRate)
            .updater(updater).adamMeanDecay(0.9).adamVarDecay(0.999)
            .optimizationAlgo(optimizationAlgorithm)
            .dropOut(dropoutRate)
            .weightInit(weightInit)
            .regularization(true).l2(l2)
            .list()
            .layer(0, new VariationalAutoencoder.Builder()
                .activation(activation)
                .encoderLayerSizes(nIn[1], nIn[2])
                .decoderLayerSizes(nOut[1], nOut[2])
                .pzxActivationFunction("identity")
                .reconstructionDistribution(new GaussianReconstructionDistribution("identity"))
                .nIn(nIn[0])
                .nOut(nOut[0]).lossFunction(lossFunctions)
                .build())
            .pretrain(true).backprop(false).build();
    }

    public MultiLayerNetwork buildModel() {
        return buildModel(conf());
    }

    public MultiLayerNetwork buildModel(MultiLayerConfiguration conf) {
        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();
        return net;
    }
}
