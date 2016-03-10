package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.util.ClassPathResource;
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
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Alex on 8/03/2016.
 */
public class TrainMLP {

    private static final Logger log = LoggerFactory.getLogger(TrainMLP.class);


    public static boolean isWin = System.getProperty("os.name").toLowerCase().contains("win");
    protected static String outputFilePath = "src/main/resources/";

    public static final String OUT_DIRECTORY = (isWin) ? "C:/Data/UNSW_NB15/Out/" :
            FilenameUtils.concat(System.getProperty("user.dir"), outputFilePath);

    public static final String TRAIN_DATA_PATH = FilenameUtils.concat(OUT_DIRECTORY,"train/normalized0.csv");
    public static final String TEST_DATA_PATH = FilenameUtils.concat(OUT_DIRECTORY,"test/normalized0.csv");

    public static final String NETWORK_SAVE_DIR = (isWin) ? "C:/Data/UNSW_NB15/Trained/" :
            FilenameUtils.concat(System.getProperty("user.dir"), outputFilePath);

//    public static String TRAIN_DATA_PATH = FilenameUtils.concat(OUT_DIRECTORY, "csv_100_preprocessed.csv");
//    public static String TEST_DATA_PATH = FilenameUtils.concat(OUT_DIRECTORY,"csv_100_test_preprocessed.csv");

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

        FileUtils.writeStringToFile(new File(NETWORK_SAVE_DIR, "config.json"), conf.toJson());
        try(DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(NETWORK_SAVE_DIR,"params.bin")))){
            Nd4j.write(net.params(),dos);
        }

        INDArray params;
        try(DataInputStream dis = new DataInputStream(new FileInputStream(new File(NETWORK_SAVE_DIR,"params.bin")))){
            params = Nd4j.read(dis);
        }

        System.out.println(params);

        log.info("----- Complete -----");
    }


}
