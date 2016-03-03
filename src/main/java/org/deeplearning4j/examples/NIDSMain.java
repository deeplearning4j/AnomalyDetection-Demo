package org.deeplearning4j.examples;


import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.examples.Models.BasicRNNModel;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.util.NetSaverLoaderUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.io.ClassPathResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(NIDSMain.class);

    // values to pass in from command line when compiled, esp running remotely
    @Option(name="--version",usage="Version to run (Standard, SparkStandAlone, SparkCluster)",aliases = "-v")
    protected String version = "Standard";
    @Option(name="--batchSize",usage="Batch size",aliases="-b")
    protected int batchSize = 40;
    @Option(name="--testBatchSize",usage="Test Batch size",aliases="-tB")
    protected int testBatchSize = batchSize;
    @Option(name="--numBatches",usage="Number of batches",aliases="-nB")
    protected int numBatches = 1;
    @Option(name="--numTestBatches",usage="Number of test batches",aliases="-nTB")
    protected int numTestBatches = numBatches;
    @Option(name="--numEpochs",usage="Number of epochs",aliases="-nE")
    protected int numEpochs = 1; // consider 60
    @Option(name="--iterations",usage="Number of iterations",aliases="-i")
    protected int iterations = 1;
    @Option(name="--numCategories",usage="Number of categories",aliases="-nC")
    protected int numCategories = 1;
    @Option(name="--trainFolder",usage="Train folder",aliases="-taF")
    protected String trainFolder = "train";
    @Option(name="--testFolder",usage="Test folder",aliases="-teF")
    protected String testFolder = "test";
    @Option(name="--saveModel",usage="Save model",aliases="-sM")
    protected boolean saveModel = false;
    @Option(name="--saveParams",usage="Save parameters",aliases="-sP")
    protected boolean saveParams = false;

    @Option(name="--confName",usage="Model configuration file name",aliases="-conf")
    protected String confName = null;
    @Option(name="--paramName",usage="Parameter file name",aliases="-param")
    protected String paramName = null;

    @Option(name="--lstmLayerSize",usage="Layer Size",aliases="-lS")
    protected int lstmLayerSize = 4;
    @Option(name="--nIn",usage="Number of activations in",aliases="-nIn")
    protected int nIn = 10;
    @Option(name="--nOut",usage="Number activations out",aliases="-nOut")
    protected int nOut = 10;
    @Option(name="--truncatedBPTTLength",usage="Truncated BPTT length",aliases="-tBPTT")
    protected int truncatedBPTTLength = 2;

    protected long startTime = 0;
    protected long endTime = 0;
    protected int trainTime = 0;
    protected int testTime = 0;

    protected static final int outputNum = 1860;
    protected int seed = 123;
    protected int listenerFreq = 1;
    protected int totalTrainNumExamples = batchSize * numBatches;
    protected int totalTestNumExamples = testBatchSize * numTestBatches;

    // Paths for data
//    protected String basePath = FilenameUtils.concat(System.getProperty("user.dir"), "src/main/resources/");
//    protected String trainPath = FilenameUtils.concat(basePath, trainFolder);
//    protected String testPath = FilenameUtils.concat(basePath, testFolder);

    protected String labelPath;
    protected String outputPath;
    protected String confPath = this.toString() + "conf.yaml";
    protected String paramPath = this.toString() + "param.bin";
    protected Map<String, String> paramPaths = new HashMap<>();

    protected List<String> labels;
    protected String[] layerIdsVGG = {"cnn1", "cnn2", "cnn3", "cnn4", "ffn1", "ffn2", "output"};
    protected int nCores = 1;


    public void run(String[] args) throws Exception {

        String trainPath = new ClassPathResource("train").getFile().getAbsolutePath();
        String testPath = new ClassPathResource("test").getFile().getAbsolutePath();
//        int sparkExamplesPerFit = 32 * nCores;

        // Parse command line arguments if they exist
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }


        MultiLayerNetwork network = new BasicRNNModel(nIn, nOut, lstmLayerSize, truncatedBPTTLength, iterations).buildModel();

        switch (version) {
            case "Standard":
                StandardNIDS standard = new StandardNIDS();
                MultipleEpochsIterator data = standard.loadData();
                network = standard.trainModel(network, data);
                standard.evaluatePerformance(network, data);
                break;
            case "SparkStandAlone":
            case "SparkCluster":
                SparkNIDS spark = new SparkNIDS();
                JavaSparkContext sc = (version == "SparkStandAlone")? spark.setupLocalSpark(): spark.setupClusterSpark();
                JavaRDD<DataSet> sparkData = spark.loadData(sc, trainPath, outputPath, batchSize * numBatches,false);
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
        if (saveModel) NetSaverLoaderUtils.saveNetworkAndParameters(net, outputPath.toString());
        if (saveParams) NetSaverLoaderUtils.saveParameters(net, layerIdsVGG, paramPaths);

    }


    public static void main(String[] args) throws Exception {
        new NIDSMain().run(args);
    }

}