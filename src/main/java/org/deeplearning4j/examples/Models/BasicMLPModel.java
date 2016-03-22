package org.deeplearning4j.examples.models;

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

/**
 *
 */
public class BasicMLPModel {

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
    protected double momentum;
    protected long seed;

    public BasicMLPModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
                         double learningRate, double l2){
        this(nIn, nOut, iterations, activation, weightInit, OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                Updater.NESTEROVS, LossFunctions.LossFunction.MCXENT, learningRate, l2, 0.9, 123);
    }

    public BasicMLPModel(int[] nIn, int[] nOut, int iterations, String activation, WeightInit weightInit,
                         OptimizationAlgorithm optimizationAlgorithm, Updater updater, LossFunctions.LossFunction lossFunctions,
                         double learningRate , double l2, double momentum, long seed) {
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
        this.momentum = momentum;
        this.seed = seed;
    }

    public MultiLayerConfiguration conf(){
        return new NeuralNetConfiguration.Builder()
                .seed(seed)
                .iterations(iterations)
                .activation(activation)
                .weightInit(weightInit)
                .optimizationAlgo(optimizationAlgorithm)
                .learningRate(learningRate)
                .momentum(momentum)
                .regularization(true)
                .l2(l2)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(nIn[0]).nOut(nOut[0]).dropOut(0.5).build())
                .layer(1, new DenseLayer.Builder().nIn(nIn[1]).nOut(nOut[1]).dropOut(0.5).build())
                .layer(2, new OutputLayer.Builder(lossFunctions).activation("softmax").nIn(nIn[2]).nOut(nOut[2]).build())
                .pretrain(false).backprop(true)
                .build();

    }

    public MultiLayerNetwork buildModel() {
        MultiLayerConfiguration conf = conf();

        MultiLayerNetwork network = new MultiLayerNetwork(conf);
        network.init();
        return network;
    }
    public ComputationGraph buildGraphModel() {

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
