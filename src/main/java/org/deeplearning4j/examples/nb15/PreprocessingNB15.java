package org.deeplearning4j.examples.nb15;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.Temp.Histograms;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.TransformSequence;
import org.deeplearning4j.examples.data.analysis.AnalyzeSpark;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.IntegerAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.LongAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.RealAnalysis;
import org.deeplearning4j.examples.data.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.dataquality.QualityAnalyzeSpark;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.filter.FilterInvalidValues;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.data.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.deeplearning4j.examples.data.transform.ConditionalTransform;
import org.deeplearning4j.examples.data.transform.string.MapAllStringsExceptListTransform;
import org.deeplearning4j.examples.data.transform.string.RemoveWhiteSpaceTransform;
import org.deeplearning4j.examples.data.transform.string.ReplaceEmptyStringTransform;
import org.deeplearning4j.examples.data.transform.string.StringMapTransform;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class PreprocessingNB15 {

    public static final String CHART_DIRECTORY = "C:/Data/UNSW_NB15/Out/Charts/";

    public static void main(String[] args) throws Exception {

        //Get the initial schema
        Schema csvSchema = NB15Util.getNB15CsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = new TransformSequence.Builder(csvSchema)
                .removeColumns("timestamp start", "timestamp end", "source ip", "destination ip")  //Don't need timestamps, we have duration. Can't really use IPs here
//                .removeColumns("timestamp start", "timestamp end")  //Don't need timestamps, we have duration. Can't really use IPs here
                .filter(new FilterInvalidValues("source port", "destination port")) //Remove examples/rows that have invalid values for these columns
                .transform(new RemoveWhiteSpaceTransform("attack category"))
                .transform(new ReplaceEmptyStringTransform("attack category", "none"))  //Replace empty strings in "attack category"
                .transform(new ReplaceEmptyIntegerWithValueTransform("count flow http methods", 0))
                .transform(new ReplaceInvalidWithIntegerTransform("count ftp commands", 0)) //Only invalid ones here are whitespace
                .transform(new ConditionalTransform("is ftp login", 1, 0, 13, Arrays.asList("ftp", "ftp-data")))
                .transform(new ReplaceEmptyIntegerWithValueTransform("count flow http methods", 0))
                .transform(new StringMapTransform("attack category", Collections.singletonMap("Backdoors","Backdoor"))) //Replace all instances of "Backdoors" with "Backdoor"
                .transform(new StringToCategoricalTransform("attack category", "none", "Exploits", "Reconnaissance", "DoS", "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis"))
                .transform(new StringToCategoricalTransform("service", "-", "dns", "http", "smtp", "ftp-data", "ftp", "ssh", "pop3", "snmp", "ssl", "irc", "radius", "dhcp"))
                .transform(new MapAllStringsExceptListTransform("transaction protocol", "other", Arrays.asList("unas","sctp","ospf", "tcp", "udp", "arp"))) //Map all protocols except these to "other" (all others have <<1000 examples)
                .transform(new StringToCategoricalTransform("transaction protocol", "unas", "sctp", "ospf", "tcp", "udp", "arp", "other"))
                .transform(new MapAllStringsExceptListTransform("state", "other", Arrays.asList("FIN", "CON", "INT", "RST", "REQ")))  //Before: CategoricalAnalysis(CategoryCounts={CLO=161, FIN=1478689, ECR=8, PAR=26, MAS=7, URN=7, ECO=96, TXD=5, CON=560588, INT=490469, RST=528, TST=8, ACC=43, REQ=9043, no=7, URH=54})
                .transform(new StringToCategoricalTransform("state", "FIN", "CON", "INT", "RST", "REQ", "other"))
                .transform(new IntegerToCategoricalTransform("label", Arrays.asList("normal", "attack")))
                .transform(new IntegerToCategoricalTransform("equal ips and ports",Arrays.asList("notEqual","equal")))
                .transform(new IntegerToCategoricalTransform("is ftp login",Arrays.asList("not ftp","ftp login")))
                .build();

        Schema finalSchema = seq.getFinalSchema(csvSchema);


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        String dataDir = "C:/DL4J/Git/AnomalyDetection-Demo/src/main/resources/";   //Subset of data
        String dataDir = "C:/Data/UNSW_NB15/CSV/";
        JavaRDD<String> rawData = sc.textFile(dataDir);

//        String inputName = "csv_50_records.txt";
//        String basePath = new ClassPathResource(inputName).getFile().getAbsolutePath();
//        JavaRDD<String> rawData = sc.textFile(basePath);


        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));


        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> processedData = executor.execute(data, seq);
        processedData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQuality(finalSchema, processedData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyze(finalSchema, processedData);
//        List<Writable> invalidIsFtpLogin = QualityAnalyzeSpark.sampleInvalidColumns(100,"is ftp login",finalSchema,processedData);
//        List<Writable> invalidSourceTCPBaseSequenceNum = QualityAnalyzeSpark.sampleInvalidColumns(100,"source TCP base sequence num",finalSchema,processedData);
//        List<Writable> invalidDestTCPBaseSequenceNum = QualityAnalyzeSpark.sampleInvalidColumns(100,"dest TCP base sequence num",finalSchema,processedData);
//        List<Writable> invalidAttack = QualityAnalyzeSpark.sampleInvalidColumns(100,"attack category",finalSchema,processedData);
        sc.close();

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("------------------------------------------");
        System.out.println("Data quality:");
        System.out.println(dqa);

        System.out.println("------------------------------------------");

        System.out.println(da);

        //TODO: analysis and histograms

//        System.out.println("Invalid is ftp login data:");
//        System.out.println(invalidIsFtpLogin);

//        System.out.println("Invalid attack:");
//        System.out.println(invalidAttack);


        //Plots!
        List<ColumnAnalysis> analysis = da.getColumnAnalysis();
        List<String> names = finalSchema.getColumnNames();
        List<ColumnType> types = finalSchema.getColumnTypes();

        for( int i=0; i<analysis.size(); i++ ){
            ColumnType type = types.get(i);
            ColumnAnalysis a = analysis.get(i);
            double[] bins;
            long[] counts;
            switch(type){
                case Integer:
                    IntegerAnalysis ia = (IntegerAnalysis)a;
                    bins = ia.getHistogramBuckets();
                    counts = ia.getHistogramBucketCounts();
                    break;
                case Long:
                    LongAnalysis la = (LongAnalysis)a;
                    bins = la.getHistogramBuckets();
                    counts = la.getHistogramBucketCounts();
                    break;
                case Double:
                    RealAnalysis ra = (RealAnalysis)a;
                    bins = ra.getHistogramBuckets();
                    counts = ra.getHistogramBucketCounts();
                    break;
                default:
                    continue;
            }

            String colName = names.get(i);


//            Histograms.plot(bins,counts,colName);
            File f = new File(CHART_DIRECTORY,colName + ".png");
            if(f.exists()) f.delete();
            Histograms.exportHistogramImage(f,bins,counts,colName,1000,650);
        }


        System.out.println();
    }

}