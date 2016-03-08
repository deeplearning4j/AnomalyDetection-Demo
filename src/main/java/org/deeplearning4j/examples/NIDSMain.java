package org.deeplearning4j.examples;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.examples.Models.BasicMLPModel;
import org.deeplearning4j.examples.Models.BasicRNNModel;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.util.NetSaverLoaderUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.nd4j.linalg.dataset.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.canova.api.util.ClassPathResource;

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
    @Option(name="--batchSize",usage="Batch size",aliases="-b")
    protected int batchSize = 128;
    @Option(name="--testBatchSize",usage="Test Batch size",aliases="-tB")
    protected int testBatchSize = batchSize;
    @Option(name="--numBatches",usage="Number of batches",aliases="-nB")
    protected int numBatches = 1; // set to max 20000 when full set
    @Option(name="--numTestBatches",usage="Number of test batches",aliases="-nTB")
    protected int numTestBatches = numBatches; // set to 2500 when full set
    @Option(name="--numEpochs",usage="Number of epochs",aliases="-nE")
    protected int numEpochs = 1; // consider 60
    @Option(name="--iterations",usage="Number of iterations",aliases="-i")
    protected int iterations = 1;
    @Option(name="--trainFile",usage="Train filename",aliases="-trFN")
    protected String trainFile = "csv_preprocessed.csv";
    @Option(name="--testFile",usage="Test filename",aliases="-teFN")
    protected String testFile = "csv_test_preprocessed.csv";
//    @Option(name="--trainFolder",usage="Train folder",aliases="-taF")
//    protected String trainFolder = "train.csv";
//    @Option(name="--testFolder",usage="Test folder",aliases="-teF")
//    protected String testFolder = "test";
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
    @Option(name="--lstmLayerSize",usage="Layer Size",aliases="-lS")
    protected int lstmLayerSize = 4;
    @Option(name="--truncatedBPTTLength",usage="Truncated BPTT length",aliases="-tBPTT")
    protected int truncatedBPTTLength = 2;

    protected long startTime = 0;
    protected long endTime = 0;
    protected int trainTime = 0;
    protected int testTime = 0;
    protected int MAX_TRAIN_MINIBATCHES = 20;
//    protected int TEST_NUM_MINIBATCHES = 2;
    protected int TEST_EVERY_N_MINIBATCHES = 5;

    protected int seed = 123;
    protected int listenerFreq = 1;
    protected int totalTrainNumExamples = batchSize * numBatches;
    protected int totalTestNumExamples = testBatchSize * numTestBatches;

    public static boolean isWin = false;
    protected static String outputFilePath = "src/main/resources/";
    protected String confPath = this.toString() + "conf.yaml";
    protected String paramPath = this.toString() + "param.bin";
    protected Map<String, String> paramPaths = new HashMap<>();

    public static final String OUT_DIRECTORY = (isWin) ? "C:/Data/UNSW_NB15/Out/" :
            FilenameUtils.concat(System.getProperty("user.dir"), outputFilePath);
    public String TRAIN_DATA_PATH = (isWin) ? FilenameUtils.concat(OUT_DIRECTORY,"train/normalized0.csv"):
            FilenameUtils.concat(OUT_DIRECTORY, trainFile);
    public String TEST_DATA_PATH = (isWin) ? FilenameUtils.concat(OUT_DIRECTORY,"test/normalized0.csv"):
            FilenameUtils.concat(OUT_DIRECTORY, testFile);

    protected List<String> labels = Arrays.asList("none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis");
    protected int labelIdx = 66;

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

//        MultiLayerNetwork network = new BasicRNNModel(nIn, nOut, lstmLayerSize, truncatedBPTTLength, iterations).buildModel();
        MultiLayerNetwork network = new BasicMLPModel(
                new int[] {nIn, 512, 512},
                new int[] {512, 512, nOut},
                iterations,
                "leakyrelu",
                WeightInit.XAVIER,
                1e-2,
                1e-6
                ).buildModel();
        network.setListeners(new ScoreIterationListener(1));

        switch (version) {
            case "Standard":
                StandardNIDS standard = new StandardNIDS();
                MultipleEpochsIterator trainData = standard.loadData(batchSize, TRAIN_DATA_PATH, labelIdx, numEpochs);
                MultipleEpochsIterator testData = standard.loadData(batchSize, TEST_DATA_PATH, labelIdx, 1);
                network = standard.trainModel(network, trainData);
                standard.evaluatePerformance(network, testData);
                break;
            case "SparkStandAlone":
            case "SparkCluster":
                SparkNIDS spark = new SparkNIDS();
                JavaSparkContext sc = (version == "SparkStandAlone")? spark.setupLocalSpark(): spark.setupClusterSpark();
                JavaRDD<DataSet> sparkData = spark.loadData(sc, TRAIN_DATA_PATH, OUT_DIRECTORY, batchSize * numBatches, false);
                SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, network);
                network = spark.trainModel(sparkNetwork, sparkData);
                sparkNetwork = new SparkDl4jMultiLayer(sc, network);
                spark.evaluatePerformance(sparkNetwork, sparkData);
                break;
        }

        saveAndPrintResults(network);
        System.out.println("****************Example finished********************");
    }

    protected void saveAndPrintResults(MultiLayerNetwork net){
        // TODO save udpaters
        System.out.println("****************************************************");
        System.out.println("Total training runtime: " + trainTime + " minutes");
        System.out.println("Total evaluation runtime: " + testTime + " minutes");
        System.out.println("****************************************************");
        if (saveModel) NetSaverLoaderUtils.saveNetworkAndParameters(net, OUT_DIRECTORY.toString());
    }


    public static void main(String[] args) throws Exception {
        new NIDSMain().run(args);
    }

}