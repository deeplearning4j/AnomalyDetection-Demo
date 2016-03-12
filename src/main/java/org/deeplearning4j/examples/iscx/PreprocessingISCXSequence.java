package org.deeplearning4j.examples.iscx;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
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
import org.deeplearning4j.examples.data.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.examples.data.transform.string.StringListToCategoricalSetTransform;
import org.deeplearning4j.examples.misc.Histograms;
import scala.collection.Seq;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**Preprocessing - playing around with sequences
 */
public class PreprocessingISCXSequence {

    protected static double FRACTION_TRAIN = 0.75;
    protected static String dataSet = "ISCX";
    protected static final DataPath PATH = new DataPath(dataSet);
    public static final String IN_DIRECTORY = PATH.IN_DIR;
    public static final String OUT_DIRECTORY = PATH.PRE_DIR;
    public static final String CHART_DIRECTORY_ORIG = PATH.CHART_DIR_ORIG;
    public static final String CHART_DIRECTORY_NORM = PATH.CHART_DIR_NORM;


    public static void main(String[] args) throws Exception {

        //Get the initial schema
        Schema csvSchema = ISCXUtil.getCsvSchema();

        //Set up the sequence of transforms:
        TransformSequence seq = new TransformSequence.Builder(csvSchema)
                .removeColumns("source payload base64", "destination payload base64")
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
                .convertToSequence("source ip",new StringComparator("start time"), SequenceSchema.SequenceType.TimeSeriesAperiodic)
                .build();

        Schema preprocessedSchema = seq.getFinalSchema(csvSchema);
        FileUtils.writeStringToFile(new File(OUT_DIRECTORY,"preprocessedDataSchema.txt"),preprocessedSchema.toString());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("ISCX");
        sparkConf.set("spark.driver.maxResultSize", "2G");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rawData = sc.textFile(IN_DIRECTORY);
        JavaRDD<Collection<Writable>> data = rawData.map(new StringToWritablesFunction(new CSVRecordReader()));

        SparkTransformExecutor executor = new SparkTransformExecutor();
        JavaRDD<Collection<Writable>> processedData = executor.execute(data, seq);
        processedData.cache();

        JavaRDD<Collection<Collection<Writable>>> sequenceData = executor.executeToSequence(data, seq);
        sequenceData.cache();

        long count = sequenceData.count();  //Expect: 2478 sequences

        List<Collection<Collection<Writable>>> sample = AnalyzeSpark.sampleSequence(20,sequenceData);

//        //Analyze the quality of the columns (missing values, etc), on a per column basis
        List<DataQualityAnalysis> timeStepDQA = QualityAnalyzeSpark.analyzeQuality(sequenceData, preprocessedSchema);
//
//        //Do analysis, on a per-column basis
//        DataAnalysis da = AnalyzeSpark.analyze(preprocessedSchema, processedData);


//        List<Writable> samplesDirection = AnalyzeSpark.sampleFromColumn(100,"direction",preprocessedSchema,processedData);
//        List<Writable> samplesUnique = AnalyzeSpark.getUnique("source TCP flags",preprocessedSchema,processedData);

        //Wait for spark to stop its console spam before printing analysis
        Thread.sleep(2000);

        System.out.println("NUMBER OF SEQUENCES: " + count);
        System.out.println("-------------------------------------\nSamples:");


        for(Collection<Collection<Writable>> s : sample ){
            for(Collection<Writable> c : s){
                System.out.println(c);
            }

            System.out.println("\n\n");
        }

        System.out.println("------------------------------------------");
        count = 0;
        while(count < 20) {
            for (DataQualityAnalysis dqa : timeStepDQA) {
                System.out.println("Data quality:");
                System.out.println(dqa);

            }
            count++;
        }
        System.out.println("\n\n");



//        System.out.println("------------------------------------------");
////
//        System.out.println("Processed data summary:");
//        System.out.println(da);
////
////        System.out.println("------------------------------------------");
////
////        System.out.println("Normalized data summary: (train)");
////        System.out.println(trainDataAnalyis);
////
////        //analysis and histograms
//        plot(preprocessedSchema, da, CHART_DIRECTORY_ORIG);
////        plot(normSchema, trainDataAnalyis, CHART_DIRECTORY_NORMALIZED);
////
////        System.out.println();
    }

    public static Pair<Schema, JavaRDD<Collection<Writable>>> normalize(Schema schema, DataAnalysis da, JavaRDD<Collection<Writable>> input,
                                                                        SparkTransformExecutor executor) {
        TransformSequence norm = new TransformSequence.Builder(schema)

                .build();

        Schema normSchema = norm.getFinalSchema(schema);
        JavaRDD<Collection<Writable>> normalizedData = executor.execute(input, norm);
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
