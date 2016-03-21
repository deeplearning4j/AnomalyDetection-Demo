package org.deeplearning4j.examples.ui;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 *
 */
public class SparkConnectFactory {

    public static String name = "NIDS";

    public static SparkConf config() {
        return config(name);
    }

    public static SparkConf config(String name){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
//        sparkConf.set("spark.driver.maxResultSize", "2G");
        sparkConf.setAppName(name);
        return sparkConf;
    }

    public static JavaSparkContext getContext(){
        return getContext(name);
    }

    public static JavaSparkContext getContext(String name){
        return new JavaSparkContext(config(name));
    }

    public static JavaStreamingContext getStreamingContext(int numSec, String name){
        return new JavaStreamingContext(config(name), Durations.seconds(numSec));    //Batches: emitted every x second
    }
}
