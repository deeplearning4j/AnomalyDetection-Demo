package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.util.ClassPathResource;
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
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.data.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.deeplearning4j.examples.data.transform.ConditionalTransform;
import org.deeplearning4j.examples.data.transform.normalize.Normalize;
import org.deeplearning4j.examples.data.transform.real.DoubleLog2Normalizer;
import org.deeplearning4j.examples.data.transform.real.DoubleMinMaxNormalizer;
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

    protected static boolean isWin = true;
    protected static String inputFilePath =  "data/NIDS/UNSW/input/";
    protected static String outputFilePath =  "data/NIDS/UNSW/preprocessed/";
    protected static String chartFilePath =  "charts/";

    public static final String IN_DIRECTORY = (isWin)? "C:/Data/UNSW_NB15/Out/" :
            FilenameUtils.concat(System.getProperty("user.home"), inputFilePath);
    public static final String OUT_DIRECTORY = (isWin)? "C:/Data/UNSW_NB15/Out/" :
            FilenameUtils.concat(System.getProperty("user.home"), outputFilePath);
    public static final String CHART_DIRECTORY_ORIG = (isWin)? "C:/Data/UNSW_NB15/Out/Charts/Orig/" :
            FilenameUtils.concat(System.getProperty("user.home"), outputFilePath + chartFilePath);
    public static final String CHART_DIRECTORY_NORMALIZED = (isWin)? "C:/Data/UNSW_NB15/Out/Charts/Norm/" :
            FilenameUtils.concat(System.getProperty("user.home"), outputFilePath + chartFilePath);

    public static void main(String[] args) throws Exception {

        //Get the initial schema
        Schema csvSchema = NB15Util.getNB15CsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = new TransformSequence.Builder(csvSchema)
                .removeColumns("timestamp start", "timestamp end", "source ip", "destination ip",  //Don't need timestamps, we have duration. Can't really use IPs here.
                        "source TCP base sequence num", "dest TCP base sequence num")       //Sequence numbers are essentially random between 0 and 4.29 billion
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
        String inputName = "csv_50_records.txt";
        String dataDir = (isWin)?  "C:/Data/UNSW_NB15/CSV/": new ClassPathResource(inputName).getFile().getAbsolutePath();
//        String dataDir = (isWin)?  "C:/Data/UNSW_NB15/CSV/": IN_DIRECTORY;
        JavaRDD<String> rawData = sc.textFile(dataDir);

        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> processedData = executor.execute(data, seq);
        processedData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQuality(finalSchema, processedData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyze(finalSchema, processedData);

        // TODO normalize double
        TransformSequence norm = new TransformSequence.Builder(finalSchema)
                //source port: ?
                //destination port: ?

                .normalize("total duration", Normalize.Log2Mean0Min, da)
                .normalize("source-dest bytes", Normalize.Log2Mean0Min, da)
                .normalize("dest-source bytes", Normalize.Log2Mean0Min, da)
                .normalize("source-dest time to live", Normalize.MinMax, da)
                .normalize("dest-source time to live", Normalize.MinMax, da)
                .normalize("source packets lost", Normalize.Log2Mean0Min, da)
                .normalize("destination packets lost", Normalize.Log2Mean0Min, da)
                .normalize("source bits per second", Normalize.Log2Mean0Min, da)
                .normalize("destination bits per second", Normalize.Log2Mean0Min, da)
                .normalize("source-destination packet count", Normalize.Log2Mean0Min, da)
                .normalize("dest-source packet count", Normalize.Log2Mean0Min, da)
                .normalize("source TCP window adv", Normalize.MinMax, da)
                .normalize("dest TCP window adv", Normalize.MinMax, da)
                .normalize("source mean flow packet size", Normalize.Log2Mean0Min, da)
                .normalize("dest mean flow packet size", Normalize.Log2Mean0Min, da)
                .normalize("transaction pipelined depth", Normalize.Log2Mean0Min, da)
                .normalize("content size", Normalize.Log2Mean0Min, da)

                .normalize("source jitter ms", Normalize.Log2Mean0Min, da)
                .normalize("dest jitter ms", Normalize.Log2Mean0Min, da)
                .normalize("source interpacket arrival time", Normalize.Log2Mean0Min, da)
                .normalize("destination interpacket arrival time", Normalize.Log2Mean0Min, da)
                .normalize("tcp setup round trip time", Normalize.Log2Mean0Min, da)
                .normalize("tcp setup time syn syn_ack", Normalize.Log2Mean0Min, da)
                .normalize("tcp setup time syn_ack ack", Normalize.Log2Mean0Min, da)
                .normalize("count time to live", Normalize.MinMax, da)  //0 to 6 in data
                .normalize("count flow http methods", Normalize.Log2Mean0Min, da) //0 to 37
                .normalize("count ftp commands", Normalize.MinMax, da)  //0 to 8
                .normalize("count same service and source", Normalize.Log2Mean0Min, da)
                .normalize("count same service and dest", Normalize.Log2Mean0Min, da)
                .normalize("count same dest", Normalize.Log2Mean0Min, da)
                .normalize("count same source", Normalize.Log2Mean0Min, da)
                .normalize("count same source addr dest port", Normalize.Log2Mean0Min, da)
                .normalize("count same dest addr source port", Normalize.Log2Mean0Min, da)
                .normalize("count same source dest address", Normalize.Log2Mean0Min, da)
                .build();

        Schema normSchema = norm.getFinalSchema(finalSchema);
        JavaRDD<Collection<Writable>> normalizedData = executor.execute(processedData, norm);
        processedData.unpersist();
        normalizedData.cache();

        DataAnalysis da2 = AnalyzeSpark.analyze(normSchema, normalizedData);

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

        System.out.println("Processed data summary:");
        System.out.println(da);

        System.out.println("------------------------------------------");

        System.out.println("Normalized data summary:");
        System.out.println(da2);

        //analysis and histograms
        plot(finalSchema, da, CHART_DIRECTORY_ORIG);
        plot(normSchema, da2, CHART_DIRECTORY_NORMALIZED);

//        System.out.println("Invalid is ftp login data:");
//        System.out.println(invalidIsFtpLogin);

//        System.out.println("Invalid attack:");
//        System.out.println(invalidAttack);


        // TODO store processedData
//        processedData.saveAsObjectFile(OUT_DIRECTORY);

        System.out.println();
    }

    public static void plot(Schema finalSchema,  DataAnalysis da, String directory) throws Exception{
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
            File f = new File(directory,colName + ".png");
            if(f.exists()) f.delete();
            Histograms.exportHistogramImage(f,bins,counts,colName,1000,650);
        }


    }

}
