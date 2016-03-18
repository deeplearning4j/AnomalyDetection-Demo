package org.deeplearning4j.examples.streaming;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.TransformProcess;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 10/03/2016.
 */
public class NIDSStreaming {

    protected static String dataSet = "UNSW_NB15";
    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);

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


    public static final int GENERATION_RATE = 20;   //connections per second

    public static void main(String[] args) throws Exception {

        Schema schema = NB15Util.getCsvSchema();
        UIDriver.setTableConverter(new NB15TableConverter(NB15Util.getCsvSchema()));

        //TODO: find a better (but still general-purspose) design for this
        Map<String,Integer> columnMap = new HashMap<>();
        columnMap.put("source-dest bytes",schema.getIndexOfColumn("source-dest bytes"));
        columnMap.put("dest-source bytes",schema.getIndexOfColumn("dest-source bytes"));
        columnMap.put("source ip",schema.getIndexOfColumn("source ip"));
        columnMap.put("destination ip",schema.getIndexOfColumn("destination ip"));
        columnMap.put("source port",schema.getIndexOfColumn("source port"));
        columnMap.put("destination port",schema.getIndexOfColumn("destination port"));
        columnMap.put("service", schema.getIndexOfColumn("service"));

        UIDriver.setColumnsMap(columnMap);

        UIDriver.setClassNames(Arrays.asList("none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis"));
        UIDriver.setServiceNames(Arrays.asList("-", "dns", "http", "smtp", "ftp-data", "ftp", "ssh", "pop3", "snmp", "ssl", "irc", "radius", "dhcp"));
        UIDriver.setNormalClassIdx(0);


        UIDriver uiDriver = UIDriver.getInstance();


        //Load config and parameters:
        String conf = FileUtils.readFileToString(new File(PARAMS_PATH));

        INDArray params;
        try(DataInputStream dis = new DataInputStream(new FileInputStream(new File(CONF_PATH)))){
            params = Nd4j.read(dis);
        }

        Thread.sleep(3000);

        TransformProcess preproc = NB15Util.getPreProcessingSequence();
        TransformProcess norm;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(PATH.NORMALIZER_FILE)))) {
            norm = (TransformProcess) ois.readObject();
        }


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15Streaming");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));    //Batches: emitted every 1 second
//        sc.checkpoint(CHECKPOINT_DIR);


        //Register our streaming object for receiving data into the system:
        //FromRawCsvReceiver handles loading raw data, normalization, and conversion of normalized training data to INDArrays
        JavaDStream<Tuple3<Long, INDArray, Collection<Writable>>> dataStream = sc.receiverStream(
                new FromRawCsvReceiver(PATH.RAW_TEST_FILE, preproc, norm, CSV_LABEL_IDX, CSV_NOUT, GENERATION_RATE));

        //Pass each instance through the network:
//        JavaDStream<Tuple3<Long, INDArray, Collection<Writable>>> predictionStream = dataStream;    //TODO
        JavaDStream<Tuple3<Long, INDArray, Collection<Writable>>> predictionStream = dataStream.mapPartitions(
                new Predict3Function(sc.sc().broadcast(conf),sc.sc().broadcast(params),64));
//        JavaDStream<Tuple3<Long, INDArray, Collection<Writable>>> predictionStream = dataStream.flatMap()

        //And finally push the predictions to the UI driver so they can be displayed:
        predictionStream.foreachRDD(new CollectAtUIDriverFunction());

        //Start streaming:
        sc.start();

        sc.awaitTermination(120000);     //For testing: only run for short period of time

        sc.close();

        System.out.println("DONE");

        System.exit(0);
    }

}
