package org.deeplearning4j.examples;


import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.datasets.nb15.ui.NB15TableConverter;
import org.deeplearning4j.examples.streaming.CollectAtUIProcessorFunction;
import org.deeplearning4j.examples.streaming.FromRawCsvReceiver;
import org.deeplearning4j.examples.streaming.Predict3Function;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.ui.UIProcessor;
import org.deeplearning4j.preprocessing.api.TransformProcess;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Assumption here: this is being submitted to spark-submit.
 *
 */
public class LaunchProcessing {

    private static final Logger log = LoggerFactory.getLogger(LaunchProcessing.class);

    private static final int EXAMPLES_PER_SEC = 20;     //Number of examples to generate per second
    private static final long DEMO_DURATION_SECONDS = 1800; //Exit after this number of seconds

    public static void main(String[] args) throws Exception {

        String location;
        if(args == null || args.length == 0){
            location = "http://localhost:8080/";
        } else {
            location = args[0];
        }
        log.info("Launching processing with DropWizard running at {}",location);

//        ClassLoader cl = ClassLoader.getSystemClassLoader();
//        URL[] urls = ((URLClassLoader)cl).getURLs();
//        for(URL url: urls)System.out.println(url.getFile());
//        System.out.println("LOGGER CLASS: " + LoggerFactory.getILoggerFactory().getClass());

        //Check to make sure we can find the files we expect:
        checkFilesPreExecution();

        File testDataFile = new ClassPathResource("test.csv").getFile();
        File networkConfigFile = new ClassPathResource("config.json").getFile();
        File networkParamsFile = new ClassPathResource("params.bin").getFile();
        File normalizerFile = new ClassPathResource("normalizerTransform.bin").getFile();


        //Various settings required here
        Schema schema = NB15Util.getCsvSchema();
        List<String> labels = NB15Util.LABELS;
        List<String> services = NB15Util.SERVICES;
        int labelIdx = NB15Util.LABELIDX;
        int normalIdx = NB15Util.NORMALIDX;
        int nIn = NB15Util.NIN;
        int nOut = NB15Util.NOUT;
        TableConverter tableConverter = new NB15TableConverter(schema);
        TransformProcess preProcessor = NB15Util.getPreProcessingProcess();
        TransformProcess norm;
        Map<String,Integer> columnMap = tableConverter.getColumnMap();
        UIProcessor.createInstance(location,labels,normalIdx,services,tableConverter,columnMap);

        //Load config and parameters:
        String conf = FileUtils.readFileToString(networkConfigFile);
        INDArray params;
        try(DataInputStream dis = new DataInputStream(new FileInputStream(networkParamsFile))){
            params = Nd4j.read(dis);
        }

        //This is a work-around for this issue: https://github.com/deeplearning4j/nd4j/issues/732
        params = Nd4j.create(params.data().asDouble(), params.shape());

        //Load the data normalizer. This (along with the preProcessor TransformProcess) is part of the feature engineering
        // + data normalization pipeline
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(normalizerFile))) {
            norm = (TransformProcess) ois.readObject();
        }


        //Set up Spark Streaming context:
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("NIDS_Demo");

        //******
        //FOR TESTING PURPOSES ONLY
        sparkConf.setMaster("local[*]");
        //******

        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));    //Batches: emitted every 1 second

        //Register our streaming object for receiving data into the system:
        //FromRawCsvReceiver handles loading raw data, normalization, and conversion of normalized training data to INDArrays
        JavaDStream<Tuple3<Long, INDArray, List<Writable>>> dataStream = sc.receiverStream(
                new FromRawCsvReceiver(testDataFile, preProcessor, norm, labelIdx, nOut, EXAMPLES_PER_SEC));

        //dataStream.print(); Debugging purposes: print the data


        //Pass each instance through the network:
        JavaDStream<Tuple3<Long, INDArray, List<Writable>>> predictionStream = dataStream.mapPartitions(
                new Predict3Function(sc.sc().broadcast(conf),sc.sc().broadcast(params),64));

        //And finally push the predictions to the UI driver so they can be displayed:
        predictionStream.foreachRDD(new CollectAtUIProcessorFunction());

        //Start streaming:
        sc.start();

        sc.awaitTermination(DEMO_DURATION_SECONDS * 1000);     //For testing: only run for short period of time

        // Close proceedures
        sc.close();
        System.out.println("DONE");
        System.exit(0);
    }

    private static void checkFilesPreExecution(){
        List<String> missingFiles = new ArrayList<>();
        //Check training data:
        try{
            File f = new ClassPathResource("test.csv").getFile();
            if(!f.exists()){
                System.out.println("Could not find test data (test.csv) on classpath");
                missingFiles.add("test.csv");
            }
        }catch(Exception e){
            System.out.println("Could not find test data (test.csv) on classpath");
            e.printStackTrace();
        }

        //Check network configuration
        try{
            File f = new ClassPathResource("config.json").getFile();
            if(!f.exists()){
                System.out.println("Could not find trained network configuration file (config.json) on classpath");
                missingFiles.add("config.json");
            }
        }catch(Exception e){
            System.out.println("Could not find trained network configuration file (config.json) on classpath");
            e.printStackTrace();
        }

        //Check network parameters:
        try{
            File f = new ClassPathResource("params.bin").getFile();
            if(!f.exists()){
                System.out.println("Could not find trained network parameters file (params.bin) on classpath");
                missingFiles.add("params.bin");
            }
        }catch(Exception e){
            System.out.println("Could not find trained network parameters file (params.bin) on classpath");
            e.printStackTrace();
        }

        //Check data normalizer:
        try{
            File f = new ClassPathResource("normalizerTransform.bin").getFile();
            if(!f.exists()){
                System.out.println("Could not find data normalizer file (normalizerTransform.bin) on classpath");
                missingFiles.add("normalizerTransform.bin");
            }
        }catch(Exception e){
            System.out.println("Could not find data normalizer file (normalizerTransform.bin) on classpath");
            e.printStackTrace();
        }

        if(missingFiles.size() > 0){
            throw new RuntimeException("Could not find required files: " + missingFiles);
        } else {
            System.out.println("OK: found required files");
        }
    }

}
