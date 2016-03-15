package org.deeplearning4j.examples.iscx;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.utils.DataPathUtil;
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
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.schema.SequenceSchema;
import org.deeplearning4j.examples.data.sequence.comparator.StringComparator;
import org.deeplearning4j.examples.data.spark.StringToWritablesFunction;
import org.deeplearning4j.examples.data.split.RandomSplit;
import org.deeplearning4j.examples.data.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.examples.data.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.normalize.Normalize;
import org.deeplearning4j.examples.data.transform.string.StringListToCategoricalSetTransform;
import org.deeplearning4j.examples.misc.Histograms;
import org.deeplearning4j.examples.misc.SparkExport;
import org.deeplearning4j.examples.misc.SparkUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**Preprocessing - playing around with sequences
 */
public class PreprocessingISCXSequence {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "ISCX";
    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);
    public static final String IN_DIRECTORY = PATH.IN_DIR; //DataPath.REPO_BASE_DIR + "TestbedMonJun14Flows.csv";
    public static final String OUT_DIRECTORY = PATH.PRE_DIR;
    public static final String CHART_DIRECTORY_ORIG = PATH.CHART_DIR_ORIG;
    public static final String CHART_DIRECTORY_NORM = PATH.CHART_DIR_NORM;
    protected static final boolean analysis = true;
    protected static final boolean trainSplit = true;


    public static void main(String[] args) throws Exception {

        //Get the initial schema
        Schema csvSchema = ISCXUtil.getCsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = new TransformSequence.Builder(csvSchema)
                .removeColumns("source payload base64", "destination payload base64")  // TODO use in nlp approach for attacks that need payload to id
                .transform(new StringToCategoricalTransform("direction", "L2L", "L2R", "R2L", "R2R"))
                .transform(new StringToCategoricalTransform("protocol name", "icmp_ip", "udp_ip", "ip", "ipv6icmp", "tcp_ip", "igmp"))
                .transform(new StringToCategoricalTransform("label", "Attack", "Normal"))
                .transform(new StringToCategoricalTransform("appName",
                        "MiscApp", "WebFileTransfer", "rexec", "Misc-Mail-Port", "Web-Port", "HTTPWeb", "Telnet", "VNC",
                        "NortonAntiVirus", "WindowsFileSharing", "IPX", "Kazaa", "SIP", "Ingres", "NFS", "Hotline",
                        "ManagementServices", "TFTP", "Unknown_TCP", "Authentication", "iChat", "SNMP-Ports", "Filenet",
                        "dsp3270", "PostgreSQL", "SNA", "IPSec", "Common-Ports", "Common-P2P-Port", "Misc-DB", "Nessus",
                        "StreamingAudio", "IRC", "AOL-ICQ", "SSL-Shell", "rsh", "Unknown_UDP", "Tacacs", "Timbuktu",
                        "SecureWeb", "XFER", "NETBEUI", "Anet", "TimeServer", "UpdateDaemon", "Blubster", "IMAP",
                        "PCAnywhere", "H.323", "Printer", "MGCP", "Google", "Squid", "Oracle", "NNTPNews",
                        "MicrosoftMediaServer", "rlogin", "OpenNap", "Citrix", "RTSP", "MDQS", "Flowgen", "MSN",
                        "NortonGhost", "Intellex", "MiscApplication", "Real", "Network-Config-Ports", "LDAP", "MS-SQL",
                        "NetBIOS-IP", "FTP", "GuptaSQLBase", "MSTerminalServices", "SunRPC", "ICMP", "Hosts2-Ns",
                        "MSN-Zone", "Webmin", "DNS", "POP-port", "IGMP", "POP", "BGP", "WebMediaVideo", "SSDP", "NTP",
                        "MSMQ", "SAP", "SMTP", "giop-ssl", "Misc-Ports", "SMS", "RPC", "PeerEnabler", "Groove", "Yahoo",
                        "WebMediaDocuments", "WebMediaAudio", "XWindows", "DNS-Port", "BitTorrent", "OpenWindows",
                        "PPTP", "SSH", "HTTPImageTransfer", "Gnutella"))

                //Possible values for source/destination TCP flags: F, S, R, A, P, U, Illegal&, Illegal8, N/A, empty string
                .transform(new StringListToCategoricalSetTransform(
                        "source TCP flags",
                        Arrays.asList("sourceTCP_F", "sourceTCP_S", "sourceTCP_R", "sourceTCP_A", "sourceTCP_P", "sourceTCP_U",
                                "sourceTCP_Illegal7", "sourceTCP_Illegal8", "sourceTCP_N/A"),
                        Arrays.asList("F", "S", "R", "A", "P", "U", "Illegal7", "Illegal8", "N/A"),
                        ";"))
                .transform(new StringListToCategoricalSetTransform(
                        "destination TCP flags",
                        Arrays.asList("destinationTCP_F", "destinationTCP_S", "destinationTCP_R", "destinationTCP_A", "destinationTCP_P",
                                "destinationTCP_U", "destinationTCP_Illegal7", "destinationTCP_Illegal8", "destinationTCP_N/A"),
                        Arrays.asList("F", "S", "R", "A", "P", "U", "Illegal7", "Illegal8", "N/A"),
                        ";"))
                //aggregate into time series by source IP, then order by start time (as String field)
//                .convertToSequence("source ip",new StringComparator("start time"), SequenceSchema.SequenceType.TimeSeriesAperiodic)
                .convertToSequence("destination ip",new StringComparator("start time"), SequenceSchema.SequenceType.TimeSeriesAperiodic)
                .build();

//        Schema preprocessedSchema = seq.getFinalSchema(csvSchema);
        Schema preprocessedSchema = seq.getFinalSchema();
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("ISCX");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);
        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Collection<Writable>>> sequenceData = executor.executeToSequence(data, seq);
        sequenceData.cache();

        long count = sequenceData.count();  //Expect: 2478 sequences

        List<Collection<Collection<Writable>>> sample = AnalyzeSpark.sampleSequence(20,sequenceData);

        DataQualityAnalysis dqa;
        DataAnalysis da;
        if(analysis) {
            //Analyze the quality of the columns (missing values, etc), on a per column basis
            dqa = QualityAnalyzeSpark.analyzeQualitySequence(preprocessedSchema, sequenceData);
            //Do analysis, on a per-column basis
            da = AnalyzeSpark.analyzeSequence(preprocessedSchema, sequenceData);
        }

        DataAnalysis trainDataAnalysis;
        Schema normSchema;
        Pair<Schema, JavaRDD<Collection<Collection<Writable>>>> trainDataNormalized;
        Pair<Schema, JavaRDD<Collection<Collection<Writable>>>> testDataNormalized;
        if(trainSplit) {
            //Do train/test split:
            List<JavaRDD<Collection<Collection<Writable>>>> allData = SparkUtils.splitData(new RandomSplit(FRACTION_TRAIN), sequenceData);
            JavaRDD<Collection<Collection<Writable>>> trainData = allData.get(0);
            JavaRDD<Collection<Collection<Writable>>> testData = allData.get(1);
            trainDataAnalysis = AnalyzeSpark.analyzeSequence(preprocessedSchema, trainData);

            //Same normalization scheme for both. Normalization scheme based only on test data, however
            trainDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, trainData, executor);
            testDataNormalized = normalize(preprocessedSchema, trainDataAnalysis, testData, executor);

            sequenceData.unpersist();
            normSchema = trainDataNormalized.getFirst();
            trainDataAnalysis = AnalyzeSpark.analyzeSequence(normSchema,trainDataNormalized.getSecond());

            //Save as CSV file
            int nSplits = 1;
            SparkExport.exportSequenceCSVLocal(DataPathUtil.TRAIN_DATA_PATH, dataSet + "normalized", nSplits, ",", trainDataNormalized.getSecond(), 12345);
            SparkExport.exportSequenceCSVLocal(DataPathUtil.TEST_DATA_PATH, dataSet + "normalized", nSplits, ",", testDataNormalized.getSecond(), 12345);
            FileUtils.writeStringToFile(new File(OUT_DIRECTORY, dataSet + "normDataSchema.txt"), normSchema.toString());
        }
        sc.close();

//        List<Writable> samplesDirection = AnalyzeSpark.sampleFromColumn(100,"direction",preprocessedSchema,processedData);
//        List<Writable> samplesUnique = AnalyzeSpark.getUnique("source TCP flags",preprocessedSchema,processedData);

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);
        if(analysis) {

            System.out.println("NUMBER OF SEQUENCES: " + count);
            System.out.println("-------------------------------------\nSamples:");
            for(Collection<Collection<Writable>> s : sample ){
                for(Collection<Writable> c : s){
                    System.out.println(c);
                }
                System.out.println("\n\n");
            }

            System.out.println("------------------------------------------");
            System.out.println("Data quality:");
            System.out.println(dqa);
            System.out.println("------------------------------------------");
            System.out.println("Processed data summary:");
            System.out.println(da);
            System.out.println("------------------------------------------");
            System.out.println("Plot data:");
            plot(preprocessedSchema, da, CHART_DIRECTORY_ORIG);
        }

        if(trainSplit) {
            System.out.println("------------------------------------------");
            System.out.println("Normalized data summary: (train)");
            System.out.println(trainDataAnalysis);
            System.out.println("------------------------------------------");
            System.out.println("Plot normalized data:");
            plot(normSchema, trainDataAnalysis, CHART_DIRECTORY_NORM);
            System.out.println();
        }

        trainDataNormalized.getSecond().unpersist();
        testDataNormalized.getSecond().unpersist();

    }

    public static Pair<Schema, JavaRDD<Collection<Collection<Writable>>>> normalize(Schema schema, DataAnalysis da, JavaRDD<Collection<Collection<Writable>>> input,
                                                                        SparkTransformExecutor executor) {
        TransformSequence norm = new TransformSequence.Builder(schema)
                .normalize("totalSourceBytes", Normalize.Log2Mean, da)
                .normalize("totalDestinationBytes", Normalize.Log2Mean, da)
                .normalize("totalDestinationPackets", Normalize.Log2Mean, da)
                //Do conversion of categorical fields to a set of one-hot columns, ready for network training:
                .categoricalToOneHot("direction", "protocol name", "appName", "sourceTCP_F", "destinationTCP_F")
                .transform(new CategoricalToIntegerTransform("label"))
                .build();

        Schema normSchema = norm.getFinalSchema();
        JavaRDD<Collection<Collection<Writable>>> normalizedData = executor.executeSequenceToSequence(input, norm);
        normalizedData.cache();
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

        if(da instanceof SequenceDataAnalysis){
            SequenceDataAnalysis sda = (SequenceDataAnalysis)da;
            double[] bins = sda.getSequenceLengthAnalysis().getHistogramBuckets();
            long[] counts = sda.getSequenceLengthAnalysis().getHistogramBucketCounts();

            File f = new File(directory, "SequenceLengths.png");
            if (f.exists()) f.delete();
            Histograms.exportHistogramImage(f, bins, counts, "Sequence Lengths", 1000, 650);
        }

    }

}
