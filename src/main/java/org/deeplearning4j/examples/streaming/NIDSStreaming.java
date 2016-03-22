package org.deeplearning4j.examples.streaming;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.TransformProcess;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDTableConverter;
import org.deeplearning4j.examples.datasets.nslkdd.NSLKDDUtil;
import org.deeplearning4j.examples.utils.SparkConnectFactory;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.datasets.nb15.ui.NB15TableConverter;
import org.deeplearning4j.examples.ui.UIDriver;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple3;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;

/**
 * Created by Alex on 10/03/2016.
 */
public class NIDSStreaming {

    public static String dataSet;

    public static final int GENERATION_RATE = 20;   //connections per second
    public static int numSec = 1;

    public static void main(String... args) throws Exception {
        DataPathUtil path = null;
        String checkpointDir;
        Schema schema;
        List<String> labels;
        List<String> services;
        int labelIdx;
        int normalIdx;
        int nIn;
        int nOut;
        TableConverter tableConverter;
        TransformProcess preProcessor;
        TransformProcess norm;

        dataSet = args[0];
        if (dataSet != null) {
            path = new DataPathUtil(dataSet);
            checkpointDir = FilenameUtils.concat(path.OUT_DIR, "/Checkpoint/");
        }

        switch(dataSet.toLowerCase()) {
            case "unsw_nb15":
                schema = NB15Util.getCsvSchema();
                labels = NB15Util.LABELS;
                services = NB15Util.SERVICES;
                labelIdx = NB15Util.LABELIDX;
                nIn = NB15Util.NIN;
                nOut = NB15Util.NOUT;
                normalIdx = NB15Util.NORMALIDX;
                tableConverter = new NB15TableConverter(schema);
                preProcessor = NB15Util.getPreProcessingProcess();
                break;
            case "nslkdd":
                schema = NSLKDDUtil.getCsvSchema();
                labels = NSLKDDUtil.LABELS;
                services = NSLKDDUtil.SERVICES;
                labelIdx = NSLKDDUtil.LABELIDX;
                nIn = NSLKDDUtil.NIN;
                nOut = NSLKDDUtil.NOUT;
                normalIdx = NSLKDDUtil.NORMALIDX;
                tableConverter = new NSLKDDTableConverter(schema);
                preProcessor = NSLKDDUtil.getPreProcessingProcess();
                break;
            default:
                throw new UnsupportedOperationException("Not implemented: " + dataSet);
        }

        Map<String,Integer> columnMap = tableConverter.getColumnMap();
        UIDriver.createInstance(labels,normalIdx,services,tableConverter,columnMap);


        //Load config and parameters:
        String conf = FileUtils.readFileToString(new File(path.NETWORK_CONFIG_FILE));

        INDArray params;
        try(DataInputStream dis = new DataInputStream(new FileInputStream(new File(path.NETWORK_PARAMS_FILE)))){
            params = Nd4j.read(dis);
        }

        Thread.sleep(3000);
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(path.NORMALIZER_FILE)))) {
            norm = (TransformProcess) ois.readObject();
        }

        JavaStreamingContext sc = SparkConnectFactory.getStreamingContext(numSec, "NIDSStreaming");

        //Register our streaming object for receiving data into the system:
        //FromRawCsvReceiver handles loading raw data, normalization, and conversion of normalized training data to INDArrays
        JavaDStream<Tuple3<Long, INDArray, List<Writable>>> dataStream = sc.receiverStream(
                new FromRawCsvReceiver(path.RAW_TEST_FILE, preProcessor, norm, labelIdx, nOut, GENERATION_RATE));

        //Pass each instance through the network:
        JavaDStream<Tuple3<Long, INDArray, List<Writable>>> predictionStream = dataStream.mapPartitions(
                new Predict3Function(sc.sc().broadcast(conf),sc.sc().broadcast(params),64));

        //And finally push the predictions to the UI driver so they can be displayed:
        predictionStream.foreachRDD(new CollectAtUIDriverFunction());

        //Start streaming:
        sc.start();

        sc.awaitTermination(120000);     //For testing: only run for short period of time

        // Close proceedures
        sc.close();
        System.out.println("DONE");
        System.exit(0);
    }

}
