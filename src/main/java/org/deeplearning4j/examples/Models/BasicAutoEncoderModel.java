package org.deeplearning4j.examples.Models;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.AutoEncoder;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.lossfunctions.LossFunctions;

/**
 *
 */

public class BasicAutoEncoderModel {

    protected int[] nIn;
    protected int[] nOut;
    protected int iterations = 1;
    protected String activation = "relu";
    protected WeightInit weightInit = WeightInit.XAVIER;
    protected OptimizationAlgorithm optimizationAlgorithm = OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT;
    protected Updater updater = Updater.NESTEROVS;
    protected LossFunctions.LossFunction autoLossFunctions = LossFunctions.LossFunction.RMSE_XENT;
    protected LossFunctions.LossFunction lossFunctions = LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD;
    protected double learningRate = 0.1;
    protected double momentum = 0.5;
    protected double l2 = 0.001;
    protected double corruptionLevel = 0.3;
    protected long seed = 123;


    public BasicAutoEncoderModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit, double learningRate, double l2){
        this(nIn, nOut, iterations, activation, weightInit, OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                Updater.NESTEROVS, LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD, learningRate, l2, 123);
    }

    public BasicAutoEncoderModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
                                 OptimizationAlgorithm optimizationAlgorithm, Updater updater, LossFunctions.LossFunction lossFunctions,
                                 double learningRate , double l2, long seed) {
        this.nIn = nIn;
        this.nOut = nOut;
        this.iterations = iterations;
        this.weightInit = weightInit;
        this.activation = activation;
        this.weightInit = weightInit;
        this.optimizationAlgorithm = optimizationAlgorithm;
        this.updater = updater;
        this.lossFunctions = lossFunctions;
        this.learningRate = learningRate;
        this.l2 = l2;
        this.seed = seed;
    }

    public MultiLayerNetwork buildModel() {
        System.out.println("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .iterations(iterations)
                .seed(seed)
                .activation(activation)
                .weightInit(weightInit)
                .optimizationAlgo(optimizationAlgorithm)
                .learningRate(learningRate)
                .momentum(momentum)
                .regularization(true)
                .l2(l2)
                .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)
                .gradientNormalizationThreshold(1.0)
                .list(3)
                .layer(0, new AutoEncoder.Builder().nIn(nIn[0]).nOut(nOut[0])
                        .corruptionLevel(corruptionLevel).lossFunction(autoLossFunctions).build())
                .layer(1, new AutoEncoder.Builder().nIn(nIn[1]).nOut(nOut[1])
                        .corruptionLevel(corruptionLevel).lossFunction(autoLossFunctions).build())
                .layer(2, new OutputLayer.Builder(lossFunctions).activation("softmax").nIn(nIn[2]).nOut(nOut[2]).build())
                .pretrain(false).backprop(true)
                .build();

        MultiLayerNetwork network = new MultiLayerNetwork(conf);
        network.init();
        return network;

    }


}
