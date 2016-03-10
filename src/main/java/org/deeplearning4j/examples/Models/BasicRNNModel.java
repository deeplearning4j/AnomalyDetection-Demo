package org.deeplearning4j.examples.Models;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.*;
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

    protected int[] nIn;
    protected int[] nOut;
    private int iterations = 1;
    protected String activation;
    protected WeightInit weightInit;
    protected OptimizationAlgorithm optimizationAlgorithm;
    protected Updater updater;
    protected LossFunctions.LossFunction lossFunctions;
    protected double learningRate;
    protected double l2;
    protected int truncatedBPTTLength;
    private long seed = 123;

    public BasicRNNModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
                         double learningRate, double l2, int truncatedBPTTLength) {
        this(nIn, nOut, iterations, activation, weightInit, OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                Updater.NESTEROVS, LossFunctions.LossFunction.MCXENT, learningRate, l2, truncatedBPTTLength, 123);
    }

    public BasicRNNModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
                         OptimizationAlgorithm optimizationAlgorithm, Updater updater, LossFunctions.LossFunction lossFunctions,
                         double learningRate , double l2, int truncatedBPTTLength, long seed) {
        this.nIn = nIn;
        this.nOut = nOut;
        this.iterations = iterations;
        this.activation = activation;
        this.weightInit = weightInit;
        this.optimizationAlgorithm = optimizationAlgorithm;
        this.updater = updater;
        this.lossFunctions = lossFunctions;
        this.learningRate = learningRate;
        this.l2 = l2;
        this.truncatedBPTTLength = truncatedBPTTLength;
        this.seed = seed;
    }

    public MultiLayerNetwork buildModel() {
        System.out.println("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .iterations(iterations)
                .weightInit(weightInit)
                .dist(new UniformDistribution(-0.08, 0.08))
                .activation(activation)
                .optimizationAlgo(optimizationAlgorithm)
                .updater(updater)
                .learningRate(learningRate)
//                .learningRateDecayPolicy(LearningRatePolicy.Exponential)
//                .lrPolicyDecayRate(0.99)
                .rmsDecay(0.95)
                .seed(seed)
                .regularization(true)
                .l2(l2)
                .list(3)
                .layer(0, new GravesLSTM.Builder().nIn(nIn[0]).nOut(nOut[0]).build())
                .layer(1, new GravesLSTM.Builder().nIn(nIn[1]).nOut(nOut[1]).build())
                .layer(2, new RnnOutputLayer.Builder(lossFunctions)
                        .activation("softmax").nIn(nIn[2]).nOut(nOut[2]).build())
                .pretrain(false).backprop(true)
                .backpropType(BackpropType.TruncatedBPTT).tBPTTForwardLength(truncatedBPTTLength).tBPTTBackwardLength(truncatedBPTTLength)
                .build();

        MultiLayerNetwork network = new MultiLayerNetwork(conf);
        network.init();
        return network;

    }


}
