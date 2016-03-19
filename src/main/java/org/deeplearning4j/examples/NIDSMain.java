package org.deeplearning4j.examples;

import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.records.reader.impl.CSVSequenceRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.deeplearning4j.datasets.canova.SequenceRecordReaderDataSetIterator;
import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.eval.Evaluation;
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
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;

import java.io.File;
import java.util.List;

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
    @Option(name="--modelType",usage="Type of model (MLP, RNN, Auto)",aliases = "-mT")
    protected String modelType = "MLP";
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
//    @Option(name="--trainFile",usage="Train filename",aliases="-trFN")
//    protected String trainFile = "0NSL_KDDnormalized0.csv";
//    @Option(name="--testFile",usage="Test filename",aliases="-teFN")
//    protected String testFile = "1NSL_KDDnormalized0.csv";
    @Option(name="--saveModel",usage="Save model",aliases="-sM")
    protected boolean saveModel = false;

    @Option(name="--nIn",usage="Number of activations in",aliases="-nIn")
    protected int nIn = 112;
    @Option(name="--nOut",usage="Number activations out",aliases="-nOut")
    protected int nOut = 40;

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
    protected boolean supervised = false;

    protected List<String> labels;
    protected int labelIdx;
    protected MultiLayerNetwork network;
    protected DataPathUtil PATH;
    protected String OUT_DIR;
    protected int TEST_EVERY_N_MINIBATCHES = (supervised)? numBatches/2: numBatches+1;

    // TODO setup approach to load models and compare... use Arbiter?
    // TODO add early stopping

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

        // set Labels
        switch(dataSet.toLowerCase()){
            case "unsw_nb15":
                labels = NB15Util.LABELS;
                labelIdx = NB15Util.LABELIDX;
                break;
            case "nslkdd":
                labels = NSLKDDUtil.LABELS;
                labelIdx = NSLKDDUtil.LABELIDX;
                break;
            default:
                throw new UnsupportedOperationException("Not implemented: " + dataSet);
        }

        System.out.println("\nLoad data....");
        boolean rnn = modelType.toLowerCase().equals("rnn");
        MultipleEpochsIterator trainData = loadData(batchSize, PATH, labelIdx, numEpochs, numBatches, nOut, true, rnn);
        MultipleEpochsIterator testData = loadData(batchSize, PATH, labelIdx, 1, numTestBatches, nOut, false, rnn);

        System.out.println("\nBuild model....");
        buildModel();

        System.out.println("Train model....");
        network = trainModel(network, trainData, testData);

        System.out.println("\nFinal evaluation....");
        if(supervised){
            evaluateSupervisedPerformance(network, testData);
        } else {
            evaluateUnsupervisedPerformance(testData);
        }

        System.out.println("\nSave model and params....");
        saveAndPrintResults(network);
        System.out.println("\n==========================Example Finished==============================");
        System.exit(0);
    }


    protected MultipleEpochsIterator loadData(int batchSize, DataPathUtil dataPath, int labelIdx, int numEpochs, int numBatches,
                                              int nOut, boolean train, boolean rnn) throws Exception{
        DataSetIterator iter;
        if(rnn){
            CSVSequenceRecordReader rr = new CSVSequenceRecordReader(0,",");
            String path = (train ? dataPath.PRE_TRAIN_DATA_DIR : dataPath.PRE_TEST_DATA_DIR);
            rr.initialize(new FileSplit(new File(path)));
            iter = new SequenceRecordReaderDataSetIterator(rr,batchSize,nOut,labelIdx,false);
        } else {

            CSVRecordReader rr = new CSVRecordReader(0,",");
            rr.initialize(new FileSplit(new File(train ? dataPath.NORM_TRAIN_DATA_FILE : dataPath.NORM_TEST_DATA_FILE)));
            iter = new RecordReaderDataSetIterator(rr, batchSize, labelIdx , nOut, numBatches);
        }

        return new MultipleEpochsIterator(numEpochs, iter);

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
                supervised = true;
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
                supervised = true;
                break;
            case "Denoise":
                network = new BasicAutoEncoderModel(
                        new int[]{nIn, 200, 100},
                        new int[]{200, 100, nOut},
                        iterations,
                        "softsign",
                        WeightInit.XAVIER,
                        OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                        Updater.SGD,
                        LossFunctions.LossFunction.MSE,
                        1e-3,
                        1e-2,
                        seed
                        ).buildModel();
                supervised = false;
                break;
            case "MLPAuto":
                network = new MLPAutoEncoderModel(
                        new int[]{nIn,50,200,300,200,50},
                        new int[]{50,200,300,200,50,nOut},
                        iterations,
                        "softplus",
                        WeightInit.XAVIER,
                        OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                        Updater.NESTEROVS,
                        LossFunctions.LossFunction.MCXENT,
                        1e-3,
                        1,
                        seed
                ).buildModel();
                supervised = false;
                break;
        }

        if(useHistogram)
            network.setListeners(new ScoreIterationListener(1), new HistogramIterationListener(1));
        else
            network.setListeners(new ScoreIterationListener(1));

    }

    protected MultiLayerNetwork trainModel(MultiLayerNetwork net, MultipleEpochsIterator iter, MultipleEpochsIterator testIter){
        org.nd4j.linalg.dataset.api.DataSet next;
        startTime = System.currentTimeMillis();

        int countTrain = 0;
        while(iter.hasNext()  && countTrain++ < numBatches) {
            next = iter.next();
            if(next == null) break;
            net.fit(next);
            if (countTrain % TEST_EVERY_N_MINIBATCHES == 0 && supervised) {
                //Test:
                log.info("--- Evaluation after {} examples ---",countTrain*batchSize);
                evaluateSupervisedPerformance(net, testIter);
                testIter.reset();
            }
        }
        if(!iter.hasNext()) iter.reset();
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return net;
    }

    protected void evaluateSupervisedPerformance(MultiLayerNetwork net, MultipleEpochsIterator iter){
        startTime = System.currentTimeMillis();
        Evaluation eval = net.evaluate(iter, labels);
        endTime = System.currentTimeMillis();
        System.out.println(eval.stats());
        System.out.print("False Alarm Rate: " + eval.falseAlarmRate());
        testTime = (int) (endTime - startTime) / 60000;

    }

    protected void evaluateUnsupervisedPerformance(MultipleEpochsIterator iter){
        org.nd4j.linalg.dataset.DataSet test = iter.next(1);
        INDArray result = network.scoreExamples(test,false);
        // TODO get summary result...
        INDArray r = result.slice(0);
        System.out.println("\nFinal evaluation score: " +  result);

    }

    protected void saveAndPrintResults(MultiLayerNetwork net){
        System.out.println("\n============================Time========================================");
        System.out.println("Training complete. Time: " + trainTime +" min");
        System.out.println("Evaluation complete. Time " + testTime +" min");
        // TODO setup to use NETWORK_CONFIG_FILE and param version from DataPath
        if (saveModel) NetSaverLoaderUtils.saveNetworkAndParameters(net, OUT_DIR);
    }


    public static void main(String... args) throws Exception {
        new NIDSMain().run(args);
    }

}