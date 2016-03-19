package org.deeplearning4j.examples;

import org.apache.commons.io.FileUtils;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
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

    protected static String dataSet;
    protected static DataPathUtil PATH;

    public static void main(String[] args) throws Exception {

        dataSet = args[0]; //"UNSW_NB15";
        PATH = new DataPathUtil(dataSet);

        int nIn = 66;
        int nOut = 10;
        int labelIdx = 66;

        int layerSize = 256;
        int minibatchSize = 128;

        List<String> labels = Arrays.asList("none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis");

        CSVRecordReader rr = new CSVRecordReader(0,",");
        rr.initialize(new FileSplit(new File(PATH.NORM_TRAIN_DATA_FILE)));
        DataSetIterator iterTrain = new RecordReaderDataSetIterator(rr,minibatchSize,labelIdx,nOut);

        CSVRecordReader rrTest = new CSVRecordReader(0,",");
        rrTest.initialize(new FileSplit(new File(PATH.NORM_TEST_DATA_FILE)));
        DataSetIterator iterTest = new RecordReaderDataSetIterator(rrTest,minibatchSize,labelIdx,nOut);

        int MAX_TRAIN_MINIBATCHES = 20000;
        int TEST_NUM_MINIBATCHES = 2500;
        int TEST_EVERY_N_MINIBATCHES = 5000;

        Nd4j.getRandom().setSeed(12345);
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .updater(Updater.NESTEROVS).momentum(0.9)
                .seed(12345)
                .iterations(1)
                .learningRate(1e-2)
                .regularization(true).l2(1e-5)
//                .activation("leakyrelu")
                .activation("relu")
                .weightInit(WeightInit.XAVIER)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(nIn).nOut(layerSize).build())
                .layer(1, new DenseLayer.Builder().nIn(layerSize).nOut(layerSize).build())
                .layer(2, new OutputLayer.Builder().lossFunction(LossFunctions.LossFunction.MCXENT)
                        .nIn(layerSize).nOut(nOut).activation("softmax").build())
                .pretrain(false).backprop(true).build();

        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();

        net.setListeners(new ScoreIterationListener(1));
//        net.setListeners(new HistogramIterationListener(10));


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
                log.info("False Alarm Rate: {}", evaluation.falseAlarmRate());

            }

            if(!iterTrain.hasNext()) iterTrain.reset();
        }
        long end = System.currentTimeMillis();
        log.info("Training complete. Time: {} min", (end - start) / 60000);

        FileUtils.writeStringToFile(new File(PATH.NETWORK_CONFIG_FILE), conf.toJson());
        try(DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(PATH.NETWORK_PARAMS_FILE)))){
            Nd4j.write(net.params(),dos);
        }

        INDArray params;
        try(DataInputStream dis = new DataInputStream(new FileInputStream(new File(PATH.NETWORK_PARAMS_FILE)))){
            params = Nd4j.read(dis);
        }

//        System.out.println(params);

        log.info("----- Complete -----");
    }


}
