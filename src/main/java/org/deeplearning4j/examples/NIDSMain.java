package org.deeplearning4j.examples;

import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.models.BasicAutoEncoderModel;
import org.deeplearning4j.examples.models.BasicMLPModel;
import org.deeplearning4j.examples.models.BasicRNNModel;
import org.deeplearning4j.examples.models.MLPAutoEncoderModel;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.ui.weights.HistogramIterationListener;
import org.deeplearning4j.util.NetSaverLoaderUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NIDS Main Class
 *
 * Take in common parameters
 * Setup model
 * Run single server model, use spark stand alone or run on spark cluster
 *
 * Steps:
 * - Download dataset and store in System.getProperty("user.home"), "data/NIDS/<name of dataset>"
 * - Run SplitTrainTestRaw and pass in the name of the dataset folder (e.g.) UNSW_NB15 or NSLKDD
 * - Run PreprocessingPreSplit and pass in the name of the dataset folder (e.g.) UNSW_NB15 or NSLKDD
 * - Run NIDSMain (or TrainMLP) and pass in values for below and/or update variables below to train and store the model
 * - Run NIDSStreaming to run SparkStreamin and access analysis GUI of streaming results
 *
 * Steps for RNN training:
 * - Download data set as above
 * - Run
 */

public class NIDSMain {

    protected static final Logger log = LoggerFactory.getLogger(NIDSMain.class);

    // values to pass in from command line when compiled, esp running remotely
    @Option(name="--version",usage="Version to run (Standard, SparkStandAlone, SparkCluster)",aliases = "-v")
    protected String version = "Standard";
    @Option(name="--modelType",usage="Type of model (MLP, RNN, Auto)",aliases = "-mT")
    protected String modelType = "MLP";
    protected boolean supervised = false;
    @Option(name="--batchSize",usage="Batch size",aliases="-b")
    protected int batchSize = 128;
    @Option(name="--testBatchSize",usage="Test Batch size",aliases="-tB")
    protected int testBatchSize = batchSize;
    @Option(name="--numBatches",usage="Number of batches",aliases="-nB")
    protected int numBatches = 200; // consider 20K
    @Option(name="--numTestBatches",usage="Number of test batches",aliases="-nTB")
    protected int numTestBatches = numBatches; // set to 2500 when full set
    @Option(name="--numEpochs",usage="Number of epochs",aliases="-nE")
    protected int numEpochs = 2; // consider 60
    @Option(name="--iterations",usage="Number of iterations",aliases="-i")
    protected int iterations = 1;
    @Option(name="--dataSet",usage="Name of dataSet folder",aliases="-dataS")
    protected static String dataSet = "NSLKDD";
    @Option(name="--trainFile",usage="Train filename",aliases="-trFN")
    protected String trainFile = "0NSL_KDDnormalized0.csv";
    @Option(name="--testFile",usage="Test filename",aliases="-teFN")
    protected String testFile = "1NSL_KDDnormalized0.csv";
    @Option(name="--saveModel",usage="Save model",aliases="-sM")
    protected boolean saveModel = false;
    @Option(name="--confName",usage="Model configuration file name",aliases="-conf")
    protected String confName = null;
    @Option(name="--paramName",usage="Parameter file name",aliases="-param")
    protected String paramName = null;

    @Option(name="--nIn",usage="Number of activations in",aliases="-nIn")
    protected int nIn = 112;
    @Option(name="--nOut",usage="Number activations out",aliases="-nOut")
    protected int nOut = 40; // 2 binary or 10 classification of attack types
    @Option(name="--truncatedBPTTLength",usage="Truncated BPTT length",aliases="-tBPTT")
    protected int truncatedBPTTLength = 20;

    @Option(name="--useHistogramListener",usage="Add a histogram iteration listener",aliases="-hist")
    protected boolean useHistogram = false;

    protected long startTime = 0;
    protected long endTime = 0;
    protected int trainTime = 0;
    protected int testTime = 0;

    protected int seed = 123;
    protected int listenerFreq = 1;
    protected int totalTrainNumExamples = batchSize * numBatches;
    protected int totalTestNumExamples = testBatchSize * numTestBatches;

    protected String confPath = this.toString() + "conf.yaml";
    protected String paramPath = this.toString() + "param.bin";
    protected Map<String, String> paramPaths = new HashMap<>();


    protected List<String> labels = NSLKDDUtil.LABELS;
    protected int labelIdx = 66;
    protected MultiLayerNetwork network;

    protected int TEST_EVERY_N_MINIBATCHES = (supervised)? numBatches/2: numBatches+ 1;
    protected DataPathUtil PATH;    // = new DataPathUtil(dataSet); //This needs to be set AFTER parsing command line args
    protected String OUT_DIR;   // = PATH.OUT_DIR;

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

        PATH = new DataPathUtil(dataSet);
        OUT_DIR = PATH.OUT_DIR;

        // TODO setup approach to load models and compare... use Arbiter?
        // TODO add early stopping
        buildModel();
        if(useHistogram){
            network.setListeners(new ScoreIterationListener(1), new HistogramIterationListener(1));
        } else {
            network.setListeners(new ScoreIterationListener(1));
        }

        switch(modelType.toLowerCase()){
            case "mlp":
            case "rnn":
                supervised = true;
        }

        switch(dataSet.toLowerCase()){
            case "unsw_nb15":
                labels = NB15Util.LABELS;
                break;
            case "nslkdd":
                labels = NSLKDDUtil.LABELS;
                break;
            default:
                throw new UnsupportedOperationException("Not implemented: " + dataSet);
        }


        switch (version) {
            case "Standard":
                StandardNIDS standard = new StandardNIDS();
                standard.labels = this.labels;
                standard.labelIdx = this.labelIdx;
                System.out.println("\nLoad data....");
                boolean rnn = modelType.toLowerCase().equals("rnn");
                MultipleEpochsIterator trainData = standard.loadData(batchSize, PATH, labelIdx, numEpochs, numBatches, nOut, true, rnn);
                MultipleEpochsIterator testData = standard.loadData(batchSize, PATH, labelIdx, 1, numTestBatches, nOut, false, rnn);
                network = standard.trainModel(network, trainData, testData);
                trainTime = standard.trainTime;
                System.out.println("\nFinal evaluation....");
                if(supervised){
                    standard.evaluatePerformance(network, testData);
                    testTime = standard.testTime;
                } else {
                    DataSet test = testData.next(1);
//                    INDArray result = network.scoreExamples(test,false);
                    // TODO get summary result...
//                    System.out.println("\nFinal evaluation score: " +  result);
                }
                break;
//            case "SparkStandAlone":
//            case "SparkCluster":
//                SparkNIDS spark = new SparkNIDS();
//                JavaSparkContext sc = (version == "SparkStandAlone")? spark.setupLocalSpark(): spark.setupClusterSpark();
//                System.out.println("\nLoad data....");
//                JavaRDD<DataSet> trainSparkData = spark.loadData(sc, DataPath.TRAIN_DATA_PATH + trainFile);
//                SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, network);
//                network = spark.trainModel(sparkNetwork, trainSparkData);
//                trainSparkData.unpersist();
//
//                sparkNetwork = new SparkDl4jMultiLayer(sc, network);
//                JavaRDD<DataSet> testSparkData = spark.loadData(sc, DataPath.TEST_DATA_PATH + testFile);
//                System.out.println("\nFinal evaluation....");
//                spark.evaluatePerformance(sparkNetwork, testSparkData);
//                testSparkData.unpersist();
//                break;
        }

        saveAndPrintResults(network);
        System.out.println("\n==========================Example Finished==============================");
        System.exit(0);
    }

    protected void buildModel(){
        // TODO generalize
//         int[] nIn;
//         int[] nOut;
//         int iterations;
//         String activation;
//         WeightInit weightInit;
//         OptimizationAlgorithm optimizationAlgorithm;
//         Updater updater;
//         LossFunctions.LossFunction lossFunctions;
//         double learningRate;
//         double l2;
//        long seed;

        switch (modelType) {
            case "MLP":
                network = new BasicMLPModel(
                        new int[]{nIn, 512, 512},
                        new int[]{512, 512, nOut},
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
                        "softsign",
                        WeightInit.XAVIER,
                        OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                        Updater.RMSPROP,
                        LossFunctions.LossFunction.MCXENT,
                        5e-3,
                        1e-5,
                        truncatedBPTTLength,
                        123
                        ).buildModel();
                break;
            case "Denoise":
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
            case "MLPAuto":
                network = new MLPAutoEncoderModel(
                        new int[]{nIn,50,200,300,200,50},
                        new int[]{50,200,300,200,50,nOut},
                        iterations,
                        "leakyrelu",
                        WeightInit.RELU,
                        1e-4,
                        1e-4
                ).buildModel();
                break;
        }

    }

    protected void saveAndPrintResults(MultiLayerNetwork net){
        System.out.println("\n============================Time========================================");
        System.out.println("Training complete. Time: " + trainTime +" min");
        System.out.println("Evaluation complete. Time " + testTime +" min");
        if (saveModel) NetSaverLoaderUtils.saveNetworkAndParameters(net, OUT_DIR);
    }


    public static void main(String... args) throws Exception {
        new NIDSMain().run(args);
    }

}