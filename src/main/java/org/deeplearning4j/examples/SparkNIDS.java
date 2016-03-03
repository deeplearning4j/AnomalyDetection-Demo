package org.deeplearning4j.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.nd4j.linalg.dataset.DataSet;

/**
 *
 */
public class SparkNIDS extends NIDSMain{


    // TODO add spark streaming

    protected JavaSparkContext setupLocalSpark(){
        SparkConf conf = new SparkConf()
                .setMaster("local[*]");
        conf.setAppName("NIDSExample Local");
        conf.set(SparkDl4jMultiLayer.AVERAGE_EACH_ITERATION, String.valueOf(true));
        return new JavaSparkContext(conf);
    }

    protected JavaSparkContext setupClusterSpark(){
        SparkConf conf = new SparkConf();
        conf.setAppName("NIDSExample Cluster");
        return new JavaSparkContext(conf);
    }


    protected JavaRDD<DataSet> loadData(JavaSparkContext sc, String inputPath, String seqOutputPath, int numberExamples, boolean save) {
        System.out.println("Load data...");
        // TODO setup data load
//        JavaRDD<String> rawStrings = sc.parallelize(list);
//        rawStrings.persist(StorageLevel.MEMORY_ONLY());
        return null;
    }

    protected MultiLayerNetwork trainModel(SparkDl4jMultiLayer model, JavaRDD<DataSet> data){
        System.out.println("Train model...");
        startTime = System.currentTimeMillis();
        model.fitDataSet(data, batchSize, totalTrainNumExamples, numBatches);
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return model.getNetwork().clone();

    }

    protected void evaluatePerformance(SparkDl4jMultiLayer model, JavaRDD<DataSet> testData) {
        System.out.println("Eval model...");
        startTime = System.currentTimeMillis();
        Evaluation evalActual = model.evaluate(testData, labels);
        System.out.println(evalActual.stats());
        endTime = System.currentTimeMillis();
        testTime = (int) (endTime - startTime) / 60000;
    }



}