package org.deeplearning4j.examples.archive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.writable.Writable;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.examples.NIDSMain;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.spark.datavec.DataVecDataSetFunction;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.nd4j.linalg.dataset.DataSet;

import java.util.Collection;
import java.util.List;

/**
 *
 */
public class SparkNIDS extends NIDSMain {

//    protected int totalTrainNumExamples = batchSize * numBatches;
//    protected int totalTestNumExamples = testBatchSize * numTestBatches;
//    protected Map<String, String> paramPaths = new HashMap<>();


    protected void init(){
        //                SparkNIDS spark = new SparkNIDS();
//                JavaSparkContext sc = (version == "SparkStandAlone")? spark.setupLocalSpark(): spark.setupClusterSpark();
//                System.out.println("\nLoad data....");
//                JavaRDD<DataSet> trainSparkData = spark.loadData(sc, DataPath.TRAIN_DATA_PATH + trainFile);
//                SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, network);
//                network = spark.trainModel(sparkNetwork, trainSparkData);
//                trainSparkData.unpersist();
//
//                sparkNetwork = new SparkDl4jMultiLayer(sc, network);
//                JavaRDD<DataSet> testSparkData = spark.loadData(sc, DataPath.TEST_DATA_PATH + testFile);
//                System.out.println("\nFinal evaluation....");
//                spark.evaluatePerformance(sparkNetwork, testSparkData);
//                testSparkData.unpersist();
    }

    protected JavaSparkContext setupLocalSpark(){
        SparkConf conf = new SparkConf()
                .setMaster("local[*]");
        conf.setAppName("NIDSExample Local");
        // conf.set(SparkDl4jMultiLayer.AVERAGE_EACH_ITERATION, String.valueOf(true));
        return new JavaSparkContext(conf);
    }

    protected JavaSparkContext setupClusterSpark(){
        SparkConf conf = new SparkConf();
        conf.setAppName("NIDSExample Cluster");
        return new JavaSparkContext(conf);
    }


//    protected JavaRDD<DataSet> loadData(JavaSparkContext sc, String dataPath) {
//        JavaRDD<String> rawStrings = sc.textFile(dataPath);
//        JavaRDD<List<Writable>> rdd = rawStrings.map(new StringToWritablesFunction(new CSVRecordReader(0,",")));
//        JavaRDD<DataSet> ds = rdd.map(new DataVecDataSetFunction(labelIdx, nOut, false));
//        ds.persist(StorageLevel.MEMORY_ONLY());
//        return ds;
//    }

    protected MultiLayerNetwork trainModel(SparkDl4jMultiLayer model, JavaRDD<DataSet> data){
        System.out.println("Train model...");
        startTime = System.currentTimeMillis();
//        model.fitDataSet(data, batchSize, totalTrainNumExamples, numBatches);
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return model.getNetwork().clone();

    }

    protected void evaluatePerformance(SparkDl4jMultiLayer model, JavaRDD<DataSet> testData) {
        startTime = System.currentTimeMillis();
        Evaluation evalActual = model.evaluate(testData, labels);
        System.out.println(evalActual.stats());
        endTime = System.currentTimeMillis();
        testTime = (int) (endTime - startTime) / 60000;
    }



}
