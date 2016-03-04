package org.deeplearning4j.examples.nb15;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.TransformationSequence;
import org.deeplearning4j.examples.data.analysis.AnalyzeSpark;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class AnalysisSpark {

    public static final String dataDir = "C:/Data/UNSW-NB15/CSV/";

    public static void main(String[] args) throws Exception {

        Schema csvSchema = new Schema.Builder()
                .addColumnString("source ip")
                .addColumnInteger("source port")
                .addColumnString("destination ip")
                .addColumnInteger("destination port")
                .addColumnsString("transaction protocol","state")
                .addColumnReal("total duration")
                .addColumnsInteger("source-dest bytes", "dest-source bytes", "source-dest time to live", "dest-source time to live",
                        "source packets lost", "destination packets lost")
                .addColumnString("service")
                .addColumnsReal("source bits per second","destination bits per second")
                .addColumnsInteger("source-destination packet count", "dest-source packet count", "source TCP window adv", "dest TCP window adv",
                        "source TCP base sequence num", "dest TCP base sequence num", "source mean flow packet size",
                        "dest mean flow packet size", "transaction pipelined depth", "content size")
                .addColumnsReal("source jitter ms", "dest jitter ms")
                .addColumnsString("timestart start", "timestamp end")
                .addColumnsReal("source interpacket arrival time", "destination interpacket arrival time", "tcp setup round trip time",
                        "tcp setup time syn syn_ack", "tcp setup time syn_ack ack")
                .addColumnsInteger("equal ips and ports", "count time to live", "count flow http methods", "is ftp login",
                        "count ftp commands", "count same service and source", "count same service and dest",
                        "count same dest", "count same source", "count same source addr dest port", "count same dest addr source port",
                        "count same source dest address")
                .addColumnString("attack category")
                .addColumnInteger("label")
                .build();

        for( int i=0; i<csvSchema.numColumns(); i++ ){
            System.out.println(i + "\t" + csvSchema.getName(i) + "\t" + csvSchema.getType(i));
        }

        //Remove some columns, just for testing purposes:
        TransformationSequence seq = new TransformationSequence.Builder()
                .removeColumns("source bits per second", "destination bits per second")
                .build();

        Schema finalSchema = seq.getFinalSchema(csvSchema);

        System.out.println("-----------------------------");
//        for( int i=0; i<finalSchema.numColumns(); i++ ){
//            System.out.println(i + "\t" + finalSchema.getName(i) + "\t" + csvSchema.getType(i));
//        }


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String subsetDir = "C:/Data/UNSW_NB15/CSV_subset/";
        JavaRDD<String> rawData = sc.textFile(subsetDir);
        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        //Data before going through the pipeline:
//        for(Collection<Writable> c : data.collect()){
//            System.out.println(c.size() + "\t" + c);
//        }

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> out = executor.execute(data,seq);

        //Data after going through the pipeline:
//        List<Collection<Writable>> collected = out.collect();
//        System.out.println("------------------------------------------");
//        for(Collection<Writable> c : collected){
//            System.out.println(c.size() + "\t" + c);
//        }

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyze(finalSchema,out);
        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");

        System.out.println(da);

        //TODO: print histogram bins. Integer, Real and String ColumnAnalysis classes have histogram functionality already implemented
    }

}
