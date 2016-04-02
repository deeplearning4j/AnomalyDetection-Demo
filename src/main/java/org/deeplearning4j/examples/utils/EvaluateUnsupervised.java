package org.deeplearning4j.examples.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.canova.api.io.data.BooleanWritable;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.StringSplit;
import org.canova.api.writable.Writable;
import org.deeplearning4j.berkeley.Pair;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.preprocessing.spark.misc.StringToWritablesFunction;
import org.deeplearning4j.spark.canova.CanovaDataSetFunction;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * Evaluate unsupervised data
 *
 * Score individual examples, sort and capture sample set based on threshold (e.g. 5%, 10%)
 * Save results for review
 */

public class EvaluateUnsupervised {

    // Pass in test data as DataSet with labels in RDD
    // Score examples by parsing out of DataSet only features

    // Supervised - compare actual with score
    // Unsupervised - take highest score with top 10% and output to file
    // (confirm results look good)

    public MultiLayerNetwork network;
    public String path;
    public JavaSparkContext sc;
    private int labelIdx;
    private Comparator comparator;
    private List sortData;
    private boolean ascending = false;
    private boolean sorted = false;


    public EvaluateUnsupervised(MultiLayerNetwork network, String path, JavaSparkContext sc, int labelIdx) {
        this.network = network;
        this.path = path;
        this.sc = (sc == null) ? SparkConnectFactory.getContext("evaluate") : sc;
        this.labelIdx = labelIdx == 0 ? -1 : labelIdx;
    }

    public EvaluateUnsupervised(MultiLayerNetwork network, String path) {
        this(network, path, null, 0);
    }

    public EvaluateUnsupervised(MultiLayerNetwork network, JavaSparkContext sc) {
        this(network, null, sc, 0);
    }

    // sort and return limited results
    public Map<Double, DataSet> sortValues(Map<DataSet, Double> scoredData, int limit){
        if(!sorted) {
            sortData = new LinkedList(scoredData.entrySet());
            Collections.sort(sortData, new Comparator() {
                public int compare(Object o1, Object o2) {
                    if (ascending) return ((Comparable) ((Map.Entry) (o1)).getValue())
                            .compareTo(((Map.Entry) (o2)).getValue());
                    return ((Comparable) ((Map.Entry) (o2)).getValue())
                            .compareTo(((Map.Entry) (o1)).getValue());
                }
            });
            sorted = true;
        }

        Map<Double, DataSet> sortedResults = new LinkedHashMap<>();
        Iterator it = sortData.iterator();
        int i = 0;
        while(it.hasNext() && i < limit){
            Map.Entry entry = (Map.Entry) it.next();
            sortedResults.put((double) entry.getValue(), (DataSet) entry.getKey());
            i++;
        }
        return sortedResults;
    }

    // sort and return limited results
    public Comparator<Pair<Double, DataSet>> sortValues(){
        if(!sorted) {
            comparator = new Comparator<Pair<Double, DataSet>>() {
                @Override
                public int compare(Pair<Double, DataSet> o1, Pair<Double, DataSet> o2) {
                    if (ascending) return Double.compare(o1.getFirst(), o2.getFirst());
                    return Double.compare(o2.getFirst(), o1.getFirst());
                }
            };
            sorted = true;
        }
        return comparator;
    }

    // score examples in iterator
    public Map<Double, DataSet> scoreExamples(MultipleEpochsIterator iter, double threshold, int numExamples) {
        Map<DataSet, Double> scoredData = new HashMap<>();
        threshold = threshold == 0.0 ? 0.1 : threshold;
        int limit = (int) threshold * numExamples;

        // and or DataSetIter takes in iter and iter for features and labels....
        while(iter.hasNext()) {
            DataSet ds = iter.next(1);
            double score = network.scoreExamples(ds, false).getDouble(0);
            scoredData.put(ds, score);
        }
        return sortValues(scoredData, limit);
    }

    // score examples in rdd
    public List<Pair<Double, DataSet>> scoreExamples(JavaRDD<DataSet> dataSet, double threshold, int numExamples) {
        threshold = threshold == 0.0 ? 0.1 : threshold;
        int limit = (int) threshold * numExamples;

        JavaRDD<Pair<Double, DataSet>> scoredData = dataSet.map(new ScoreMapFunction(
                sc.broadcast(network.getLayerWiseConfigurations().toJson()), sc.broadcast(network.params())));
        return scoredData.takeOrdered(limit, sortValues());
    }

    // load data to rdd
    public JavaRDD<DataSet> loadData() {
        if (sc == null)
            throw new IllegalArgumentException("Reinitialize EvaluateUnsupervised with attributes to setup Spark.");
        JavaRDD<String> rawStrings = sc.textFile(path);
        JavaRDD<Collection<Writable>> rdd = rawStrings.map(new Function<String, Collection<Writable>>() {
            @Override
            public Collection<Writable> call(String s) throws Exception {
                RecordReader recordReader = new CSVRecordReader(0, ",");
                recordReader.initialize(new StringSplit(s));
                return recordReader.next();
            }
        });

        int nOut = network.getOutputLayer().getParam("b").shape()[0];
        JavaRDD<DataSet> ds = rdd.map(new CanovaDataSetFunction(labelIdx, nOut, false));
        ds.persist(StorageLevel.MEMORY_ONLY());
        return ds;

    }

    public void resetSort(){ sorted = false;}

    // save files
    public void save(List<Pair<Double, DataSet>> data, String storePath){
    }

    public void save(Map<Double, DataSet> data, String storePath){
    }

}
