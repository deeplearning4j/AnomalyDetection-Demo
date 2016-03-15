package org.deeplearning4j.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.spark.canova.CanovaDataSetFunction;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.nd4j.linalg.dataset.DataSet;

import java.util.Collection;

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


    protected JavaRDD<DataSet> loadData(JavaSparkContext sc, String dataPath) {
        JavaRDD<String> rawStrings = sc.textFile(dataPath);
        JavaRDD<Collection<Writable>> rdd = rawStrings.map(new StringToWritablesFunction(new CSVRecordReader(0,",")));
        JavaRDD<DataSet> ds = rdd.map(new CanovaDataSetFunction(labelIdx, nOut, false));
        ds.persist(StorageLevel.MEMORY_ONLY());
        return ds;
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
        startTime = System.currentTimeMillis();
        Evaluation evalActual = model.evaluate(testData, labels);
        System.out.println(evalActual.stats());
        endTime = System.currentTimeMillis();
        testTime = (int) (endTime - startTime) / 60000;
    }



}
