package org.deeplearning4j.examples.Models;

import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.ComputationGraphConfiguration;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.util.List;

/**
 *
 */
public class BasicMLPModel {

    protected int[] nIn;
    protected int[] nOut;
    protected int iterations = 1;
    protected String activation = "relu";
    protected WeightInit weightInit = WeightInit.XAVIER;
    protected OptimizationAlgorithm optimizationAlgorithm = OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT;
    protected Updater updater = Updater.ADAGRAD;
    protected LossFunctions.LossFunction lossFunctions = LossFunctions.LossFunction.MSE;
    protected double learningRate = 0.05;
    protected double l2 = 0.0001;
    protected long seed = 123;


    public BasicMLPModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit, double learningRate, double l2){
        this(nIn, nOut, iterations, activation, weightInit, OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                Updater.SGD, LossFunctions.LossFunction.MSE, learningRate, l2, 123);
    }

    public BasicMLPModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
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
                .activation(activation)
                .weightInit(weightInit)
                .optimizationAlgo(optimizationAlgorithm)
                .learningRate(learningRate)
                .seed(seed)
                .regularization(true)
                .l2(l2)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(nIn[0]).nOut(nOut[0]).build())
                .layer(1, new DenseLayer.Builder().nIn(nIn[1]).nOut(nOut[1]).build())
                .layer(2, new OutputLayer.Builder(lossFunctions).activation("softmax").nIn(nIn[2]).nOut(nOut[2]).build())
                .pretrain(false).backprop(true)
                .build();

        MultiLayerNetwork network = new MultiLayerNetwork(conf);
        network.init();
        return network;
    }

    public ComputationGraph buildGraphModel() {
        System.out.println("Build model....");

        ComputationGraphConfiguration conf = new NeuralNetConfiguration.Builder()
                .iterations(iterations)
                .activation(activation)
                .weightInit(weightInit)
                .optimizationAlgo(optimizationAlgorithm)
                .learningRate(learningRate)
                .seed(seed)
                .regularization(true)
                .l2(l2)
                .graphBuilder()
                .addLayer("dnn1", new DenseLayer.Builder().nIn(nIn[0]).nOut(nOut[0]).build(), "input")
                .addLayer("dnn2", new DenseLayer.Builder().nIn(nIn[1]).nOut(nOut[1]).build(), "dnn2")
                .addLayer("output", new OutputLayer.Builder(lossFunctions).activation("softmax")
                        .nIn(nIn[2]).nOut(nOut[2]).build())
                .setOutputs("output")
                .pretrain(false).backprop(true)
                .build();

        ComputationGraph network = new ComputationGraph(conf);
        network.init();
        return network;

    }


}
