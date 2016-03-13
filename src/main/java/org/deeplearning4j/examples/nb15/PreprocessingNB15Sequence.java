package org.deeplearning4j.examples.nb15;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.DataPath;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.TransformSequence;
import org.deeplearning4j.examples.data.analysis.AnalyzeSpark;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.analysis.SequenceDataAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.IntegerAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.LongAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.RealAnalysis;
import org.deeplearning4j.examples.data.dataquality.DataQualityAnalysis;
import org.deeplearning4j.examples.data.dataquality.QualityAnalyzeSpark;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.filter.FilterInvalidValues;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.schema.SequenceSchema;
import org.deeplearning4j.examples.data.sequence.comparator.StringComparator;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.data.transform.ConditionalTransform;
import org.deeplearning4j.examples.data.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.examples.data.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.examples.data.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.deeplearning4j.examples.data.transform.normalize.Normalize;
import org.deeplearning4j.examples.data.transform.string.MapAllStringsExceptListTransform;
import org.deeplearning4j.examples.data.transform.string.RemoveWhiteSpaceTransform;
import org.deeplearning4j.examples.data.transform.string.ReplaceEmptyStringTransform;
import org.deeplearning4j.examples.data.transform.string.StringMapTransform;
import org.deeplearning4j.examples.misc.Histograms;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.misc.SparkUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class PreprocessingNB15Sequence {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "UNSW_NB15";
    protected static final DataPath PATH = new DataPath(dataSet);
    public static final String IN_DIRECTORY = PATH.IN_DIR;
    public static final String OUT_DIRECTORY = PATH.PRE_DIR;
    public static final String CHART_DIRECTORY_ORIG = PATH.CHART_DIR_ORIG;
    public static final String CHART_DIRECTORY_NORM = PATH.CHART_DIR_NORM;

    public static void main(String[] args) throws Exception {
        // For AWS
        if(DataPath.AWS) {
            // pull down raw
//            S3Downloader s3Down = new S3Downloader();
//            MultipleFileDownload mlpDown = s3Down.downloadFolder(s3Bucket, s3KeyPrefixOut, new File(System.getProperty("user.home") + inputFilePath));
//            mlpDown.waitForCompletion();
        }

        //Get the initial schema
        Schema csvSchema = NB15Util.getNB15CsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = new TransformSequence.Builder(csvSchema)
//                .removeColumns("timestamp start", "timestamp end", //Don't need timestamps, we have duration
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
//                .transform(new IntegerToCategoricalTransform("label", Arrays.asList("normal", "attack")))
                .transform(new IntegerToCategoricalTransform("equal ips and ports", Arrays.asList("notEqual", "equal")))
                .transform(new IntegerToCategoricalTransform("is ftp login", Arrays.asList("not ftp", "ftp login")))
                .convertToSequence("destination ip",new StringComparator("timestamp end"), SequenceSchema.SequenceType.TimeSeriesAperiodic)
                .build();

        Schema preprocessedSchema = seq.getFinalSchema(csvSchema);
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("NB15");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);

        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Collection<Writable>>> processedData = executor.executeToSequence(data, seq);
        processedData.cache();

        //Analyze the quality of the columns (missing values, etc), on a per column basis
        DataQualityAnalysis dqa = QualityAnalyzeSpark.analyzeQualitySequence(preprocessedSchema, processedData);

        //Do analysis, on a per-column basis
        DataAnalysis da = AnalyzeSpark.analyzeSequence(preprocessedSchema, processedData);

        //Do train/test split:
        List<JavaRDD<Collection<Collection<Writable>>>> allData = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN), processedData);
        JavaRDD<Collection<Collection<Writable>>> trainData = allData.get(0);
        JavaRDD<Collection<Collection<Writable>>> testData = allData.get(1);

        DataAnalysis trainDataAnalysis = AnalyzeSpark.analyzeSequence(preprocessedSchema, trainData);

        //Same normalization scheme for both. Normalization scheme based only on test data, however
        Pair<Schema, JavaRDD<Collection<Collection<Writable>>>> trainDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, trainData, executor);
        Pair<Schema, JavaRDD<Collection<Collection<Writable>>>> testDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, testData, executor);

        processedData.unpersist();
        trainDataNormalized.getSecond().cache();
        testDataNormalized.getSecond().cache();
        Schema normSchema = trainDataNormalized.getFirst();


        SequenceDataAnalysis trainDataAnalyis = AnalyzeSpark.analyzeSequence(normSchema, trainDataNormalized.getSecond());

        //Save as CSV file  -> TODO for sequences
        int nSplits = 1;
//        SparkExport.exportCSVLocal(OUT_DIRECTORY + "train/", "normalized", nSplits, ",", trainDataNormalized.getSecond(), 12345);
//        SparkExport.exportCSVLocal(OUT_DIRECTORY + "test/", "normalized", nSplits, ",", testDataNormalized.getSecond(), 12345);
//        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"normDataSchema.txt"),normSchema.toString());

//        List<Writable> invalidIsFtpLogin = QualityAnalyzeSpark.sampleInvalidColumns(100,"is ftp login",finalSchema,processedData);
        sc.close();


        if(DataPath.AWS) {
            // load preprocessed
            throw new UnsupportedOperationException();
//            S3Uploader s3Up = new S3Uploader();
//            MultipleFileUpload mlpUp = s3Up.uploadFolder(s3Bucket, s3KeyPrefixIn, new File(System.getProperty("user.home") + outputFilePath), true);
//            mlpUp.waitForCompletion();

        }

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
        plot(preprocessedSchema, da, CHART_DIRECTORY_ORIG);
        plot(normSchema, trainDataAnalyis, CHART_DIRECTORY_NORM);

        System.out.println();
    }

    public static Pair<Schema, JavaRDD<Collection<Collection<Writable>>>> normalize(Schema schema, DataAnalysis da, JavaRDD<Collection<Collection<Writable>>> input,
                                                                        SparkTransformExecutor executor) {
        TransformSequence norm = new TransformSequence.Builder(schema)
                .normalize("source port", Normalize.MinMax, da)
                .normalize("destination port", Normalize.MinMax, da)
                .normalize("total duration", Normalize.Log2Mean, da)
                .normalize("source-dest bytes", Normalize.Log2Mean, da)
                .normalize("dest-source bytes", Normalize.Log2Mean, da)
                .normalize("source-dest time to live", Normalize.MinMax, da)
                .normalize("dest-source time to live", Normalize.MinMax, da)
                .normalize("source packets lost", Normalize.Log2Mean, da)
                .normalize("destination packets lost", Normalize.Log2Mean, da)
                .normalize("source bits per second", Normalize.Log2Mean, da)
                .normalize("destination bits per second", Normalize.Log2Mean, da)
                .normalize("source-destination packet count", Normalize.Log2Mean, da)
                .normalize("dest-source packet count", Normalize.Log2Mean, da)
                .normalize("source TCP window adv", Normalize.MinMax, da)           //raw data: 0 or 255 -> 0 or 1
                .normalize("dest TCP window adv", Normalize.MinMax, da)
                .normalize("source mean flow packet size", Normalize.Log2Mean, da)
                .normalize("dest mean flow packet size", Normalize.Log2Mean, da)
                .normalize("transaction pipelined depth", Normalize.Log2MeanExcludingMin, da)   //2.33M are 0
                .normalize("content size", Normalize.Log2Mean, da)

                .normalize("source jitter ms", Normalize.Log2MeanExcludingMin, da)      //963k are 0
                .normalize("dest jitter ms", Normalize.Log2MeanExcludingMin, da)        //900k are 0
                .normalize("source interpacket arrival time", Normalize.Log2MeanExcludingMin, da)       //OK, but just to keep in line with the below
                .normalize("destination interpacket arrival time", Normalize.Log2MeanExcludingMin, da)  //500k are 0
                .normalize("tcp setup round trip time", Normalize.Log2MeanExcludingMin, da)     //1.05M are 0
                .normalize("tcp setup time syn syn_ack", Normalize.Log2MeanExcludingMin, da)    //1.05M are 0
                .normalize("tcp setup time syn_ack ack", Normalize.Log2MeanExcludingMin, da)    //1.06M are 0
                .normalize("count time to live", Normalize.MinMax, da)  //0 to 6 in data
                .normalize("count flow http methods", Normalize.Log2MeanExcludingMin, da) //0 to 37; vast majority (2.33M of 2.54M) are 0
                .normalize("count ftp commands", Normalize.MinMax, da)  //0 to 8; only 43k are non-zero
                .normalize("count same service and source", Normalize.Log2Mean, da)
                .normalize("count same service and dest", Normalize.Log2Mean, da)
                .normalize("count same dest", Normalize.Log2Mean, da)
                .normalize("count same source", Normalize.Log2Mean, da)
                .normalize("count same source addr dest port", Normalize.Log2MeanExcludingMin, da)              //1.69M ore the min value of 1.0
                .normalize("count same dest addr source port", Normalize.Log2MeanExcludingMin, da) //1.97M of 2.54M are the minimum value of 1.0
                .normalize("count same source dest address", Normalize.Log2Mean, da)

                //Do conversion of categorical fields to a set of one-hot columns, ready for network training:
                .categoricalToOneHot("transaction protocol", "state", "service", "equal ips and ports", "is ftp login")
                .transform(new CategoricalToIntegerTransform("attack category"))
                .build();

        Schema normSchema = norm.getFinalSchema(schema);
        JavaRDD<Collection<Collection<Writable>>> normalizedData = executor.executeSequenceToSequence(input, norm);
        return new Pair<>(normSchema, normalizedData);
    }

    public static void plot(Schema finalSchema, DataAnalysis da, String directory) throws Exception {
        //Plots!
        List<ColumnAnalysis> analysis = da.getColumnAnalysis();
        List<String> names = finalSchema.getColumnNames();
        List<ColumnType> types = finalSchema.getColumnTypes();

        for (int i = 0; i < analysis.size(); i++) {
            ColumnType type = types.get(i);
            ColumnAnalysis a = analysis.get(i);
            double[] bins;
            long[] counts;
            switch (type) {
                case Integer:
                    IntegerAnalysis ia = (IntegerAnalysis) a;
                    bins = ia.getHistogramBuckets();
                    counts = ia.getHistogramBucketCounts();
                    break;
                case Long:
                    LongAnalysis la = (LongAnalysis) a;
                    bins = la.getHistogramBuckets();
                    counts = la.getHistogramBucketCounts();
                    break;
                case Double:
                    RealAnalysis ra = (RealAnalysis) a;
                    bins = ra.getHistogramBuckets();
                    counts = ra.getHistogramBucketCounts();
                    break;
                default:
                    continue;
            }

            String colName = names.get(i);


//            Histograms.plot(bins,counts,colName);
            File f = new File(directory, colName + ".png");
            if (f.exists()) f.delete();
            Histograms.exportHistogramImage(f, bins, counts, colName, 1000, 650);
        }


    }

}
