package org.deeplearning4j.examples;

import org.apache.commons.io.FileUtils;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.records.reader.impl.CSVSequenceRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.deeplearning4j.datasets.canova.SequenceRecordReaderDataSetIterator;
import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.earlystopping.EarlyStoppingConfiguration;
import org.deeplearning4j.earlystopping.EarlyStoppingModelSaver;
import org.deeplearning4j.earlystopping.EarlyStoppingResult;
import org.deeplearning4j.earlystopping.saver.LocalFileModelSaver;
import org.deeplearning4j.earlystopping.scorecalc.DataSetLossCalculator;
import org.deeplearning4j.earlystopping.termination.MaxEpochsTerminationCondition;
import org.deeplearning4j.earlystopping.termination.MaxTimeIterationTerminationCondition;
import org.deeplearning4j.earlystopping.trainer.EarlyStoppingTrainer;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.examples.dataProcessing.PreprocessingPreSplit;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.models.BasicAutoEncoderModel;
import org.deeplearning4j.examples.models.BasicMLPModel;
import org.deeplearning4j.examples.models.BasicRNNModel;
import org.deeplearning4j.examples.models.MLPAutoEncoderModel;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.ui.weights.HistogramIterationListener;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * NIDS Main Class
 **
 * Steps:
 * - Download dataset and store in System.getProperty("user.home"), "data/NIDS/<name of dataset>"
 *      - https://console.aws.amazon.com/s3/home?region=us-west-2#&bucket=anomaly-data&prefix=nids/UNSW/input/
 *      - https://www.unsw.adfa.edu.au/australian-centre-for-cyber-security/cybersecurity/ADFA-NB15-Datasets/
 * - Run PreprocessingPreSplit and pass in the name of the dataset folder (e.g.) UNSW_NB15 or NSLKDD
 * - Run NIDSMain and pass in values for below and/or update variables below to train and store the model
 * - Run NIDSStreaming to run SparkStreaming and access analysis GUI of streaming results
 *
 * OR
 *
 * - Download dataset
 * - Run FullPipelineRun
 *
 * Steps for RNN training:
 * - Download data set as above
 * - Run ...
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
    protected int numBatches = 20000; // consider 20K
    @Option(name="--numTestBatches",usage="Number of test batches",aliases="-nTB")
    protected int numTestBatches = 2500; // set to 2500 when full set
    @Option(name="--numEpochs",usage="Number of epochs",aliases="-nE")
    protected int numEpochs = 2; // consider 60
    @Option(name="--iterations",usage="Number of iterations",aliases="-i")
    protected int iterations = 1;
    @Option(name="--dataSet",usage="Name of dataSet folder",aliases="-dataS")
    protected static String dataSet = "NSLKDD";
    @Option(name="--preProcess",usage="Preprocess model",aliases="-pM")
    protected boolean preProcess = false;
    @Option(name="--earlyStop",usage="Apply early stop",aliases="-eS")
    protected boolean earlyStop = false;
    @Option(name="--saveModel",usage="Save model",aliases="-sM")
    protected boolean saveModel = false;
    @Option(name="--useHistogramListener",usage="Add a histogram iteration listener",aliases="-hist")
    protected boolean useHistogram = false;

    @Option(name="--nIn",usage="Number of activations in",aliases="-nIn")
    protected int nIn = 66;
    @Option(name="--nOut",usage="Number activations out",aliases="-nOut")
    protected int nOut = 10;

    @Option(name="--truncatedBPTTLength",usage="Truncated BPTT length",aliases="-tBPTT")
    protected int truncatedBPTTLength = 20;

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
    protected MultiLayerConfiguration conf;
    protected DataPathUtil PATH;
    protected String OUT_DIR;
    protected int TEST_EVERY_N_MINIBATCHES = 5000;
    protected int TEST_EVERY_N_EPOCHS = 20;

    // TODO setup approach to load models and compare... use Arbiter?

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
        switch (dataSet.toLowerCase()) {
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

        if (preProcess) {
            System.out.println("\nPreprocess data....");
            PreprocessingPreSplit.main(dataSet);
        }

        System.out.println("\nLoad data....");
        boolean rnn = modelType.toLowerCase().equals("rnn");
        MultipleEpochsIterator trainData = loadData(batchSize, PATH, labelIdx, numEpochs, numBatches, nOut, true, rnn);
        MultipleEpochsIterator testData = loadData(testBatchSize, PATH, labelIdx, 1, numTestBatches, nOut, false, rnn);

        System.out.println("\nBuild model....");
        buildModel();

        System.out.println("Train model....");

        if (earlyStop) {
            EarlyStoppingConfiguration esConf = earlyStopConfig(testData);
            EarlyStoppingTrainer trainer = new EarlyStoppingTrainer(esConf, conf, trainData);
            EarlyStoppingResult result = trainer.fit();
            printEarlyStopResults(result);
        } else {
            if(useHistogram)
                network.setListeners(new ScoreIterationListener(listenerFreq), new HistogramIterationListener(listenerFreq));
            else
                network.setListeners(new ScoreIterationListener(listenerFreq));

            network = trainModel(network, trainData, testData);

            System.out.println("\nFinal evaluation....");
            if (supervised) {
                evaluateSupervisedPerformance(network, testData);
            } else {
                evaluateUnsupervisedPerformance(testData);
            }

            System.out.println("\nSave model and params....");
            saveAndPrintResults(network);
            System.out.println("\n==========================Example Finished==============================");
            System.exit(0);
        }
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
        switch (modelType) {
            case "MLP":
                // NSLKDD got .98% with 512, RMSProp & leakyrelu
                BasicMLPModel mlpmodel = new BasicMLPModel(
                        new int[]{nIn, 256, 256},
                        new int[]{256, 256, nOut},
                        iterations,
                        "leakyrelu",
                        WeightInit.XAVIER,
                        OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                        Updater.NESTEROVS,
                        LossFunctions.LossFunction.MCXENT,
                        1e-2,
                        1e-5,
                        1,
                        seed);
                conf = mlpmodel.conf();
                network = mlpmodel.buildModel(conf);
                supervised = true;
                break;
            case "RNN":
                // reference: http://sacj.cs.uct.ac.za/index.php/sacj/article/viewFile/248/150
                // partial config for this and from examples repo
                // use 1000 epochs
                BasicRNNModel rnnmodel = new BasicRNNModel(
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
                        seed);
                conf = rnnmodel.conf();
                network = rnnmodel.buildModel(conf);
                supervised = true;
                break;
            case "Denoise":
                BasicAutoEncoderModel aemodel = new BasicAutoEncoderModel(
                        new int[]{nIn, 60, 30},
                        new int[]{60, 30, nOut},
                        iterations,
                        "sigmoid",
                        WeightInit.XAVIER,
                        OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT,
                        Updater.ADAM,
                        LossFunctions.LossFunction.MSE,
                        5e-4,
                        1e-2,
                        0.3,
                        seed);
                conf = aemodel.conf();
                network = aemodel.buildModel(conf);
                supervised = false;
                break;
            case "MLPAuto":
                MLPAutoEncoderModel maemodel = new MLPAutoEncoderModel(
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
                        seed);
                conf = maemodel.conf();
                network = maemodel.buildModel(conf);
                supervised = false;
                break;
        }

    }

    protected EarlyStoppingConfiguration earlyStopConfig(DataSetIterator iter){
        EarlyStoppingModelSaver saver = new LocalFileModelSaver(PATH.EARLY_STOP_DIR);
        return new EarlyStoppingConfiguration.Builder()
                .epochTerminationConditions(new MaxEpochsTerminationCondition(numEpochs)) //Max of 50 epochs
                .evaluateEveryNEpochs(TEST_EVERY_N_EPOCHS)
                .iterationTerminationConditions(new MaxTimeIterationTerminationCondition(10, TimeUnit.MINUTES)) //Max of 10 minutes
                .scoreCalculator(new DataSetLossCalculator(iter, true))     //Calculate test set score
                .modelSaver(saver)
                .build();
    }

    protected void printEarlyStopResults(EarlyStoppingResult result){
        System.out.println("Termination reason: " + result.getTerminationReason());
        System.out.println("Termination details: " + result.getTerminationDetails());
        System.out.println("Total epochs: " + result.getTotalEpochs());
        System.out.println("Best epoch number: " + result.getBestModelEpoch());
        System.out.println("Score at best epoch: " + result.getBestModelScore());

        //Print score vs. epoch
        Map<Integer,Double> scoreVsEpoch = result.getScoreVsEpoch();
        List<Integer> list = new ArrayList<>(scoreVsEpoch.keySet());
        Collections.sort(list);
        System.out.println("Score vs. Epoch:");
        for( Integer i : list){
            System.out.println(i + "\t" + scoreVsEpoch.get(i));
        }
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

        INDArray r = result.slice(0);
        System.out.println("\nSingle evaluation score: " +  result);

        DataSetLossCalculator calc = new DataSetLossCalculator(iter, true);
        System.out.println("\nAverage evaluation score: " +  calc.calculateScore(network));
    }

    protected void saveAndPrintResults(MultiLayerNetwork net) {
        System.out.println("\n============================Time========================================");
        System.out.println("Training complete. Time: " + trainTime +" min");
        System.out.println("Evaluation complete. Time " + testTime +" min");
        // Save config
        try {
            String config = net.getLayerWiseConfigurations().toJson();
            FileUtils.writeStringToFile(new File(PATH.NETWORK_CONFIG_FILE), config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Save params
        try(DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(PATH.NETWORK_PARAMS_FILE)))){
            Nd4j.write(net.params(),dos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Save Updater
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(PATH.NETWORK_UPDATER_FILE)))){
            oos.writeObject(net.getUpdater());
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    public static void main(String... args) throws Exception {
        new NIDSMain().run(args);
    }

}