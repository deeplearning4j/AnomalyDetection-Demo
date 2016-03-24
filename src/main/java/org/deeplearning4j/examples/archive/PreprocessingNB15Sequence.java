package org.deeplearning4j.examples.archive;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.berkeley.Triple;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.utils.DataPathUtil;
import org.deeplearning4j.preprocessing.api.TransformProcess;
import org.deeplearning4j.preprocessing.api.analysis.DataAnalysis;
import org.deeplearning4j.preprocessing.api.analysis.SequenceDataAnalysis;
import org.deeplearning4j.preprocessing.api.dataquality.DataQualityAnalysis;
import org.deeplearning4j.preprocessing.api.filter.FilterInvalidValues;
import org.deeplearning4j.preprocessing.api.charts.Histograms;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.preprocessing.api.schema.SequenceSchema;
import org.deeplearning4j.preprocessing.api.sequence.SplitMaxLengthSequence;
import org.deeplearning4j.preprocessing.api.sequence.comparator.StringComparator;
import org.deeplearning4j.preprocessing.api.split.RandomSplit;
import org.deeplearning4j.preprocessing.api.transform.ConditionalTransform;
import org.deeplearning4j.preprocessing.api.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.preprocessing.api.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.preprocessing.api.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.preprocessing.api.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.deeplearning4j.preprocessing.api.transform.string.MapAllStringsExceptListTransform;
import org.deeplearning4j.preprocessing.api.transform.string.RemoveWhiteSpaceTransform;
import org.deeplearning4j.preprocessing.api.transform.string.ReplaceEmptyStringTransform;
import org.deeplearning4j.preprocessing.api.transform.string.StringMapTransform;
import org.deeplearning4j.preprocessing.spark.AnalyzeSpark;
import org.deeplearning4j.preprocessing.spark.SparkTransformExecutor;
import org.deeplearning4j.preprocessing.spark.misc.StringToWritablesFunction;
import org.deeplearning4j.preprocessing.spark.utils.SparkExport;
import org.deeplearning4j.preprocessing.spark.utils.SparkUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class PreprocessingNB15Sequence {

    public static final long RNG_SEED = 12345;
    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "UNSW_NB15";
    public static final DataPathUtil PATH = new DataPathUtil(dataSet);
    public static final String IN_DIRECTORY = PATH.IN_DIR;
    public static final String OUT_DIRECTORY = PATH.PRE_DIR;
    public static final String CHART_DIRECTORY_ORIG = PATH.CHART_DIR_ORIG;
    public static final String CHART_DIRECTORY_NORM = PATH.CHART_DIR_NORM;

    public static void main(String[] args) throws Exception {
        //Get the initial schema
        Schema csvSchema = NB15Util.getCsvSchema();

        //Set up the sequence of transforms:
        TransformProcess seq = new TransformProcess.Builder(csvSchema)
                .removeColumns(
                        "source TCP base sequence num", "dest TCP base sequence num",       //Sequence numbers are essentially random between 0 and 4.29 billion
                        "label")    //leave attack category
                .filter(new FilterInvalidValues("source port", "destination port")) //Remove examples/rows that have invalid values for these columns
                .transform(new RemoveWhiteSpaceTransform("attack category"))
                .transform(new ReplaceEmptyStringTransform("attack category", "none"))  //Replace empty strings in "attack category"
                .transform(new ReplaceEmptyIntegerWithValueTransform("count flow http methods", 0))
                .transform(new ReplaceInvalidWithIntegerTransform("count ftp commands", 0)) //Only invalid ones here are whitespace
                .transform(new ConditionalTransform("is ftp login", 1, 0, "service", Arrays.asList("ftp", "ftp-data")))
                .transform(new ReplaceEmptyIntegerWithValueTransform("count flow http methods", 0))
                .transform(new StringMapTransform("attack category", Collections.singletonMap("Backdoors", "Backdoor"))) //Replace all instances of "Backdoors" with "Backdoor"
                .transform(new StringToCategoricalTransform("attack category", "none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis"))
                .transform(new StringToCategoricalTransform("service", "-", "dns", "http", "smtp", "ftp-data", "ftp", "ssh", "pop3", "snmp", "ssl", "irc", "radius", "dhcp"))
                .transform(new MapAllStringsExceptListTransform("transaction protocol", "other", Arrays.asList("unas", "sctp", "ospf", "tcp", "udp", "arp"))) //Map all protocols except these to "other" (all others have <<1000 examples)
                .transform(new StringToCategoricalTransform("transaction protocol", "unas", "sctp", "ospf", "tcp", "udp", "arp", "other"))
                .transform(new MapAllStringsExceptListTransform("state", "other", Arrays.asList("FIN", "CON", "INT", "RST", "REQ")))  //Before: CategoricalAnalysis(CategoryCounts={CLO=161, FIN=1478689, ECR=8, PAR=26, MAS=7, URN=7, ECO=96, TXD=5, CON=560588, INT=490469, RST=528, TST=8, ACC=43, REQ=9043, no=7, URH=54})
                .transform(new StringToCategoricalTransform("state", "FIN", "CON", "INT", "RST", "REQ", "other"))
                .transform(new IntegerToCategoricalTransform("equal ips and ports", Arrays.asList("notEqual", "equal")))
                .transform(new IntegerToCategoricalTransform("is ftp login", Arrays.asList("not ftp", "ftp login")))
                .convertToSequence("destination ip",new StringComparator("timestamp end"), SequenceSchema.SequenceType.TimeSeriesAperiodic)
                .splitSequence(new SplitMaxLengthSequence(1000,false))
                .removeColumns("timestamp start", "timestamp end", "source ip", "destination ip") //Don't need timestamps, except for ordering time steps within each sequence; don't need IPs (except for conversion to sequence)
                .build();

        Schema preprocessedSchema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);

        JavaRDD<List<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<List<List<Writable>>> processedData = executor.executeToSequence(data, seq);
        processedData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = AnalyzeSpark.analyzeQualitySequence(preprocessedSchema, processedData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyzeSequence(preprocessedSchema, processedData);

        //Do train/test split:
        List<JavaRDD<List<List<Writable>>>> allData = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN), processedData, RNG_SEED);
        JavaRDD<List<List<Writable>>> trainData = allData.get(0);
        JavaRDD<List<List<Writable>>> testData = allData.get(1);

        DataAnalysis trainDataAnalysis = AnalyzeSpark.analyzeSequence(preprocessedSchema, trainData);

        //Same normalization scheme for both. Normalization scheme based only on test data, however
        Triple<TransformProcess, Schema, JavaRDD<List<List<Writable>>>> trainDataNormalized = NB15Util.normalizeSequence(preprocessedSchema, trainDataAnalysis, trainData, executor);
        Triple<TransformProcess, Schema, JavaRDD<List<List<Writable>>>> testDataNormalized = NB15Util.normalizeSequence(preprocessedSchema, trainDataAnalysis, testData, executor);

        processedData.unpersist();
        trainDataNormalized.getThird().cache();
        testDataNormalized.getThird().cache();
        Schema normSchema = trainDataNormalized.getSecond();


        SequenceDataAnalysis trainDataAnalyis = AnalyzeSpark.analyzeSequence(normSchema, trainDataNormalized.getThird());

        //Save as CSV files
        SparkExport.exportCSVSequenceLocal(new File(PATH.PRE_TRAIN_DATA_DIR),trainDataNormalized.getThird(), RNG_SEED);
        SparkExport.exportCSVSequenceLocal(new File(PATH.PRE_TEST_DATA_DIR),testDataNormalized.getThird(), RNG_SEED);
        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");
        System.out.println("Data quality:");
        System.out.println(dqa);

        System.out.println("------------------------------------------");

        System.out.println("Processed data summary:");
        System.out.println(da);

        System.out.println("------------------------------------------");

        System.out.println("Normalized data summary: (train)");
        System.out.println(trainDataAnalyis);

        //analysis and histograms
        Histograms.exportPlots(preprocessedSchema, da, CHART_DIRECTORY_ORIG);
        Histograms.exportPlots(normSchema, trainDataAnalyis, CHART_DIRECTORY_NORM);

        System.out.println();
    }
}
