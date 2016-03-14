package org.deeplearning4j.examples.nb15.streaming;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.DataPath;
import org.deeplearning4j.examples.data.TransformSequence;
import org.deeplearning4j.examples.nb15.NB15Util;
import org.deeplearning4j.examples.nb15.ui.NB15TableConverter;
import org.deeplearning4j.examples.ui.UIDriver;
import org.nd4j.linalg.api.ndarray.INDArray;
import scala.Tuple3;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;

/**
 * Created by Alex on 10/03/2016.
 */
public class NB15Streaming {

    protected static String dataSet = "UNSW_NB15";
    protected static final DataPath PATH = new DataPath(dataSet);

    private static final boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    private static final String WIN_DIR = "C:/Data/UNSW_NB15/";

    private static final String DIR = PATH.OUT_DIR;
    private static final String MODEL_DIR = FilenameUtils.concat(DIR, "Trained/");


    private static final String CONF_FILE = FilenameUtils.concat(MODEL_DIR, "config.json");
    private static final String PARAMS_FILE = FilenameUtils.concat(MODEL_DIR, "params.bin");
//    private static final String TEST_DATA_FILE = FilenameUtils.concat(DIR, "Out/test/normalized0.csv");


    private static final String CONF_PATH = FilenameUtils.concat(DIR, PARAMS_FILE);
    private static final String PARAMS_PATH = FilenameUtils.concat(DIR, CONF_FILE);

    private static final String CHECKPOINT_DIR = FilenameUtils.concat(DIR, "/Checkpoint/");

    private static final int CSV_LABEL_IDX = 66;
    private static final int CSV_NOUT = 10;

    public static void main(String[] args) throws Exception {

        UIDriver.setTableConverter(new NB15TableConverter(NB15Util.getNB15CsvSchema()));
        UIDriver uiDriver = UIDriver.getInstance();


        //Load config and parameters:
//        String conf = FileUtils.readFileToString(new File(PARAMS_PATH));
//
//        INDArray params;
//        try(DataInputStream dis = new DataInputStream(new FileInputStream(new File(CONF_PATH)))){
//            params = Nd4j.read(dis);
//        }

        Thread.sleep(3000);

        TransformSequence preproc = NB15Util.getNB15PreProcessingSequence();
        TransformSequence norm;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(PATH.NORMALIZER_FILE)))) {
            norm = (TransformSequence) ois.readObject();
        }


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15Streaming");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));    //Batches: emitted every 1 second
        sc.checkpoint(CHECKPOINT_DIR);


        //Register our streaming object for receiving data into the system:
        //FromRawCsvReceiver handles loading raw data, normalization, and conversion of normalized training data to INDArrays
        JavaDStream<Tuple3<Long, INDArray, Collection<Writable>>> dataStream = sc.receiverStream(
                new FromRawCsvReceiver(PATH.RAW_TEST_PATH, preproc, norm, CSV_LABEL_IDX, CSV_NOUT, 10));

        //Pass each instance through the network:
        JavaDStream<Tuple3<Long, INDArray, Collection<Writable>>> predictionStream = dataStream;    //TODO

        //And finally push the predictions to the UI driver so they can be displayed:
        predictionStream.foreachRDD(new CollectAtUIDriverFunction());

        //Start streaming:
        sc.start();

        sc.awaitTermination(10000);     //For testing: only run for short period of time

        System.out.println("DONE");

    }

}
