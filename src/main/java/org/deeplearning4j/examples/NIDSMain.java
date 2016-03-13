package org.deeplearning4j.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.examples.Models.BasicAutoEncoderModel;
import org.deeplearning4j.examples.Models.BasicMLPModel;
import org.deeplearning4j.examples.Models.BasicRNNModel;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.util.NetSaverLoaderUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NIDS Main Class
 *
 * Take in common parameters
 * Setup model
 * Run single server model, use spark stand alone or run on spark cluster
 */
public class NIDSMain {

    protected static final Logger log = LoggerFactory.getLogger(NIDSMain.class);

    // values to pass in from command line when compiled, esp running remotely
    @Option(name="--version",usage="Version to run (Standard, SparkStandAlone, SparkCluster)",aliases = "-v")
    protected String version = "Standard";
    @Option(name="--modelType",usage="Type of model (MLP, RNN, Auto)",aliases = "-mT")
    protected String modelType = "MLP";
    @Option(name="--batchSize",usage="Batch size",aliases="-b")
    protected int batchSize = 128;
    @Option(name="--testBatchSize",usage="Test Batch size",aliases="-tB")
    protected int testBatchSize = batchSize;
    @Option(name="--numBatches",usage="Number of batches",aliases="-nB")
    protected int numBatches = 3; // set to max 20000 when full set
    @Option(name="--numTestBatches",usage="Number of test batches",aliases="-nTB")
    protected int numTestBatches = numBatches; // set to 2500 when full set
    @Option(name="--numEpochs",usage="Number of epochs",aliases="-nE")
    protected int numEpochs = 1; // consider 60
    @Option(name="--iterations",usage="Number of iterations",aliases="-i")
    protected int iterations = 2;
    @Option(name="--trainFile",usage="Train filename",aliases="-trFN")
    protected String trainFile = "normalized0.csv";
    @Option(name="--testFile",usage="Test filename",aliases="-teFN")
    protected String testFile = "normalized0.csv";
    @Option(name="--saveModel",usage="Save model",aliases="-sM")
    protected boolean saveModel = false;

    @Option(name="--confName",usage="Model configuration file name",aliases="-conf")
    protected String confName = null;
    @Option(name="--paramName",usage="Parameter file name",aliases="-param")
    protected String paramName = null;

    @Option(name="--nIn",usage="Number of activations in",aliases="-nIn")
    protected int nIn = 66;
    @Option(name="--nOut",usage="Number activations out",aliases="-nOut")
    protected int nOut = 10; // 2 binary or 10 classification of attack types
    @Option(name="--truncatedBPTTLength",usage="Truncated BPTT length",aliases="-tBPTT")
    protected int truncatedBPTTLength = 2;

    protected long startTime = 0;
    protected long endTime = 0;
    protected int trainTime = 0;
    protected int testTime = 0;
    protected int TEST_EVERY_N_MINIBATCHES = 2 * numEpochs;

    protected int seed = 123;
    protected int listenerFreq = 1;
    protected int totalTrainNumExamples = batchSize * numBatches;
    protected int totalTestNumExamples = testBatchSize * numTestBatches;

    public static final String TRAIN_DIR = new DataPath("UNSW_NB15").PRE_DIR;
    protected static String outputFilePath = DataPath.REPO_BASE_DIR;
    protected String confPath = this.toString() + "conf.yaml";
    protected String paramPath = this.toString() + "param.bin";
    protected Map<String, String> paramPaths = new HashMap<>();


    protected List<String> labels = Arrays.asList("none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis");
    protected int labelIdx = 66;
    protected MultiLayerNetwork network;

    public void run(String[] args) throws Exception {
        // Parse command line arguments if they exist
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }

        buildModel();
        network.setListeners(new ScoreIterationListener(1));

        switch (version) {
            case "Standard":
                StandardNIDS standard = new StandardNIDS();
                System.out.println("\nLoad data....");
                MultipleEpochsIterator trainData = standard.loadData(batchSize, TRAIN_DIR + trainFile, labelIdx, numEpochs, numBatches);
                MultipleEpochsIterator testData = standard.loadData(batchSize, TRAIN_DIR + testFile, labelIdx, 1, numTestBatches);
                network = standard.trainModel(network, trainData, testData);
                System.out.println("\nFinal evaluation....");
                standard.evaluatePerformance(network, testData);
                break;
            case "SparkStandAlone":
            case "SparkCluster":
                SparkNIDS spark = new SparkNIDS();
                JavaSparkContext sc = (version == "SparkStandAlone")? spark.setupLocalSpark(): spark.setupClusterSpark();
                System.out.println("\nLoad data....");
                JavaRDD<DataSet> trainSparkData = spark.loadData(sc, DataPath.TRAIN_DATA_PATH + trainFile);
                SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, network);
                network = spark.trainModel(sparkNetwork, trainSparkData);
                trainSparkData.unpersist();

                sparkNetwork = new SparkDl4jMultiLayer(sc, network);
                JavaRDD<DataSet> testSparkData = spark.loadData(sc, DataPath.TEST_DATA_PATH + testFile);
                System.out.println("\nFinal evaluation....");
                spark.evaluatePerformance(sparkNetwork, testSparkData);
                testSparkData.unpersist();
                break;
        }

        saveAndPrintResults(network);
        System.out.println("\n==========================Example Finished==============================");
    }

    protected void buildModel(){
        switch (modelType) {
            case "MLP":
                network = new BasicMLPModel(
                        new int[]{nIn, 500, 100},
                        new int[]{500, 100, nOut},
                        iterations,
                        "leakyrelu",
                        WeightInit.XAVIER,
                        1e-2,
                        1e-6
                ).buildModel();
                break;
            case "RNN":
                // reference: http://sacj.cs.uct.ac.za/index.php/sacj/article/viewFile/248/150
                // partial config for this and from examples repo
                // use 1000 epochs
                network = new BasicRNNModel(
                        new int[]{nIn, 10, 10},
                        new int[]{10, 10, nOut},
                        iterations,
                        "tanh",
                        WeightInit.DISTRIBUTION,
                        OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                        Updater.RMSPROP,
                        LossFunctions.LossFunction.MCXENT,
                        5e-1,
                        1,
                        truncatedBPTTLength,
                        123
                        ).buildModel();
                break;
            case "Auto":
                network = new BasicAutoEncoderModel(
                        new int[]{nIn, 500, 100},
                        new int[]{500, 100, nOut},
                        iterations,
                        "relu",
                        WeightInit.XAVIER,
                        1e-1,
                        1e-3
                        ).buildModel();
                break;
        }

    }

    protected void saveAndPrintResults(MultiLayerNetwork net){
        System.out.println("\n============================Time========================================");
        System.out.println("Training complete. Time: " + trainTime +" min");
        System.out.println("Evaluation complete. Time " + testTime +" min");
        if (saveModel) NetSaverLoaderUtils.saveNetworkAndParameters(net, new DataPath("UNSW").OUT_DIR.toString());
    }


    public static void main(String[] args) throws Exception {
        new NIDSMain().run(args);
    }

}