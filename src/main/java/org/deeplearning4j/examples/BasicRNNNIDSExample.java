package org.deeplearning4j.examples;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.distribution.UniformDistribution;
import org.deeplearning4j.nn.conf.layers.GravesLSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.util.NetSaverLoaderUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Basic RNN NIDS Example
 *
 * Resource: http://sacj.cs.uct.ac.za/index.php/sacj/article/viewFile/248/150
 * Dataset used: KDD Cup '99
 */
public class BasicRNNNIDSExample {

    private static final Logger log = LoggerFactory.getLogger(BasicRNNNIDSExample.class);

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
//        String trainPath = FilenameUtils.concat(new ClassPathResource("train").getFile().getAbsolutePath());
//        String testPath = FilenameUtils.concat(new ClassPathResource("test").getFile().getAbsolutePath());
        int sparkExamplesPerFit = 32 * nCores;


        // Parse command line arguments if they exist
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }

        JavaSparkContext sc = (version == "SparkStandAlone")? setupLocalSpark(): setupClusterSpark();

//        loadData();
        MultiLayerNetwork network = buildModel();
        SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, network);
//        trainModel();
//        evaluatePerformance();


    }


    private JavaSparkContext setupLocalSpark(){
        SparkConf conf = new SparkConf()
                .setMaster("local[*]");
        conf.setAppName("NIDSExample Local");
        conf.set(SparkDl4jMultiLayer.AVERAGE_EACH_ITERATION, String.valueOf(true));
        return new JavaSparkContext(conf);
    }

    private JavaSparkContext setupClusterSpark(){
        SparkConf conf = new SparkConf();
        conf.setAppName("NIDSExample Cluster");
        return new JavaSparkContext(conf);
    }

    private MultipleEpochsIterator loadData(int batchSize, int totalNumExamples, String mode) {
        System.out.println("Load data....");
//        return new MultipleEpochsIterator(numEpochs,...);
        return null;
    }

    private JavaRDD<DataSet> loadData(JavaSparkContext sc, String inputPath, String seqOutputPath, int numberExamples, boolean save) {
        System.out.println("Load data...");
//        JavaRDD<String> rawStrings = sc.parallelize(list);
//        rawStrings.persist(StorageLevel.MEMORY_ONLY());
        return null;
    }

    protected MultiLayerNetwork buildModel() {
        System.out.println("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
                .learningRate(0.1)
                .rmsDecay(0.95)
                .seed(12345)
                .regularization(true)
                .l2(0.001)
                .list()
                .layer(0, new GravesLSTM.Builder().nIn(nIn).nOut(lstmLayerSize)
                        .updater(Updater.RMSPROP)
                        .activation("tanh").weightInit(WeightInit.DISTRIBUTION)
                        .dist(new UniformDistribution(-0.08, 0.08)).build())
                .layer(1, new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
                        .updater(Updater.RMSPROP)
                        .activation("tanh").weightInit(WeightInit.DISTRIBUTION)
                        .dist(new UniformDistribution(-0.08, 0.08)).build())
                .layer(2, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT).activation("softmax")        //MCXENT + softmax for classification
                        .updater(Updater.RMSPROP)
                        .nIn(lstmLayerSize).nOut(nOut).weightInit(WeightInit.DISTRIBUTION)
                        .dist(new UniformDistribution(-0.08, 0.08)).build())
                .pretrain(false).backprop(true)
                .backpropType(BackpropType.TruncatedBPTT).tBPTTForwardLength(truncatedBPTTLength).tBPTTBackwardLength(truncatedBPTTLength)
                .build();

        MultiLayerNetwork network = new MultiLayerNetwork(conf);
        network.init();
        return network;

    }

    private MultiLayerNetwork trainModel(MultiLayerNetwork model, MultipleEpochsIterator data){
        System.out.println("Train model....");
        startTime = System.currentTimeMillis();
        model.fit(data);
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return model;
    }

    private MultiLayerNetwork trainModel(SparkDl4jMultiLayer model, JavaRDD<DataSet> data){
        System.out.println("Train model...");
        startTime = System.currentTimeMillis();
        model.fitDataSet(data, batchSize, totalTrainNumExamples, numBatches);
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return model.getNetwork().clone();

    }

    private void evaluatePerformance(DataSetIterator iter, MultiLayerNetwork model){
        System.out.println("Evaluate model....");
        DataSet imgNet;
        INDArray output;

        Evaluation eval = new Evaluation(labels);
        startTime = System.currentTimeMillis();
        //TODO setup iterator to randomize and pass in iterator vs doing a loop here
        for(int i=0; i < numTestBatches; i++){
            imgNet = iter.next(testBatchSize);
            output = model.output(imgNet.getFeatureMatrix());
            eval.eval(imgNet.getLabels(), output);
        }
        endTime = System.currentTimeMillis();
        System.out.println(eval.stats());
        System.out.println("****************************************************");
        testTime = (int) (endTime - startTime) / 60000;

    }

    private void evaluatePerformance(SparkDl4jMultiLayer model, JavaRDD<DataSet> testData) {
        System.out.println("Eval model...");
        startTime = System.currentTimeMillis();
        Evaluation evalActual = model.evaluate(testData, labels);
        System.out.println(evalActual.stats());
        endTime = System.currentTimeMillis();
        testTime = (int) (endTime - startTime) / 60000;
    }

    protected void saveAndPrintResults(MultiLayerNetwork net){
        System.out.println("****************************************************");
        System.out.println("Total training runtime: " + trainTime + " minutes");
        System.out.println("Total evaluation runtime: " + testTime + " minutes");
        System.out.println("****************************************************");
        if (saveModel) NetSaverLoaderUtils.saveNetworkAndParameters(net, outputPath.toString());
        if (saveParams) NetSaverLoaderUtils.saveParameters(net, layerIdsVGG, paramPaths);

    }
    

    public static void main(String[] args) throws Exception {
        new BasicRNNNIDSExample().run(args);
    }

}