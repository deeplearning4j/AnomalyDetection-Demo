package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FilenameUtils;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.ui.weights.HistogramIterationListener;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Alex on 8/03/2016.
 */
public class TrainMLP {

    private static final Logger log = LoggerFactory.getLogger(TrainMLP.class);


    public static boolean isWin = false;
    protected static String inputFilePath = "data/NIDS/UNSW/input/";
    protected static String outputFilePath = "data/NIDS/UNSW/preprocessed/";
    protected static String chartFilePath = "charts/";

    public static final String IN_DIRECTORY = (isWin) ? "C:/Data/UNSW_NB15/Out/" :
            FilenameUtils.concat(System.getProperty("user.home"), inputFilePath);
    public static final String OUT_DIRECTORY = (isWin) ? "C:/Data/UNSW_NB15/Out/" :
            FilenameUtils.concat(System.getProperty("user.home"), outputFilePath);
    public static final String CHART_DIRECTORY_ORIG = (isWin) ? "C:/Data/UNSW_NB15/Out/Charts/Orig/" :
            FilenameUtils.concat(System.getProperty("user.home"), outputFilePath + chartFilePath);
    public static final String CHART_DIRECTORY_NORMALIZED = (isWin) ? "C:/Data/UNSW_NB15/Out/Charts/Norm/" :
            FilenameUtils.concat(System.getProperty("user.home"), outputFilePath + chartFilePath);

    public static final String TRAIN_DATA_PATH = FilenameUtils.concat(OUT_DIRECTORY,"train/normalized0.csv");
    public static final String TEST_DATA_PATH = FilenameUtils.concat(OUT_DIRECTORY,"test/normalized0.csv");


    public static void main(String[] args) throws Exception {

        int nIn = 66;
        int nOut = 10;
        int labelIdx = 66;

        int layerSize = 512;
        int minibatchSize = 128;

        List<String> labels = Arrays.asList("none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis");

        CSVRecordReader rr = new CSVRecordReader(0,",");
        rr.initialize(new FileSplit(new File(TRAIN_DATA_PATH)));
        DataSetIterator iterTrain = new RecordReaderDataSetIterator(rr,minibatchSize,labelIdx,nOut);

        CSVRecordReader rrTest = new CSVRecordReader(0,",");
        rrTest.initialize(new FileSplit(new File(TEST_DATA_PATH)));
        DataSetIterator iterTest = new RecordReaderDataSetIterator(rrTest,minibatchSize,labelIdx,nOut);

        int MAX_TRAIN_MINIBATCHES = 20000;
        int TEST_NUM_MINIBATCHES = 2500;
        int TEST_EVERY_N_MINIBATCHES = 5000;


        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .updater(Updater.NESTEROVS).momentum(0.9)
                .iterations(1)
                .learningRate(1e-2)
                .regularization(true).l2(1e-6)
                .activation("leakyrelu")
                .weightInit(WeightInit.XAVIER)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(nIn).nOut(layerSize).build())
                .layer(1, new DenseLayer.Builder().nIn(layerSize).nOut(layerSize).build())
                .layer(2, new OutputLayer.Builder().lossFunction(LossFunctions.LossFunction.MCXENT)
                        .nIn(layerSize).nOut(nOut).activation("softmax").build())
                .pretrain(false).backprop(true).build();

        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();

//        net.setListeners(new HistogramIterationListener(1));
        net.setListeners(new ScoreIterationListener(1));

        log.info("Start training");
        long start = System.currentTimeMillis();

        int countTrain = 0;

        while(iterTrain.hasNext() && countTrain++ < MAX_TRAIN_MINIBATCHES){
            net.fit(iterTrain.next());

            if(countTrain % TEST_EVERY_N_MINIBATCHES == 0){
                iterTest.reset();
                //Test:
                Evaluation evaluation = new Evaluation(labels);
                int countEval = 0;
                while(iterTest.hasNext() && countEval++ < TEST_NUM_MINIBATCHES){
                    DataSet ds = iterTest.next();
                    evaluation.eval(ds.getLabels(),net.output(ds.getFeatureMatrix()));
                }

                log.info("--- Evaluation after {} examples ---",countTrain*minibatchSize);
                log.info(evaluation.stats());
            }

            if(!iterTrain.hasNext()) iterTrain.reset();
        }
        long end = System.currentTimeMillis();
        log.info("Training complete. Time: {} min", (end - start) / 60000);

        log.info("----- Complete -----");
    }


}
