package org.deeplearning4j.examples.nb15.streaming;

import jodd.io.FileNameUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Created by Alex on 10/03/2016.
 */
public class NB15Streaming {

    private static final boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    private static final String WIN_DIR = "C:/Data/UNSW_NB15/";
    private static final String MAC_DIR =  FilenameUtils.concat(System.getProperty("user.home"),"data/NIDS/UNSW/");

    private static final String DIR = WIN ? WIN_DIR : MAC_DIR;
    private static final String MODEL_DIR = FilenameUtils.concat(DIR,"Trained/");


    private static final String CONF_FILE = FileNameUtil.concat(MODEL_DIR, "config.json");
    private static final String PARAMS_FILE = FileNameUtil.concat(MODEL_DIR, "params.bin");
    private static final String TEST_DATA_FILE = FilenameUtils.concat(DIR,"Out/test/normalized0.csv");


    private static final String CONF_PATH = FilenameUtils.concat(DIR, PARAMS_FILE);
    private static final String PARAMS_PATH = FilenameUtils.concat(DIR, CONF_FILE);
    private static final String DATA_PATH = FilenameUtils.concat(DIR, TEST_DATA_FILE);

    private static final String CHECKPOINT_DIR = FilenameUtils.concat(DIR, "Checkpoint/");

    private static final int CSV_LABEL_IDX = 66;
    private static final int CSV_NOUT = 10;

    public static void main(String[] args) throws Exception {


        //Load config and parameters:
        String conf = FileUtils.readFileToString(new File(PARAMS_PATH));

        INDArray params;
        try(DataInputStream dis = new DataInputStream(new FileInputStream(new File(CONF_PATH)))){
            params = Nd4j.read(dis);
        }


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15Streaming");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));    //Batches: emitted every 1 second

        sc.checkpoint(CHECKPOINT_DIR);
        
        //Register our streaming object:
        JavaDStream<Tuple2<Long,INDArray>> dataStream = sc.receiverStream(new FromCsvReceiver(DATA_PATH,CSV_LABEL_IDX,CSV_NOUT,10));


        JavaDStream<Tuple2<Long,INDArray>> predictions = dataStream.mapPartitions(new PredictFunction(sc.sc().broadcast(conf),sc.sc().broadcast(params)));
        predictions.print();

        //Start streaming:
        sc.start();

        sc.awaitTermination(10000);     //For testing: only run for short period of time

        System.out.println("DONE");

    }

}
