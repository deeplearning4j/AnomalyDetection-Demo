package org.deeplearning4j.examples.datasets.nb15;

import org.apache.spark.api.java.JavaRDD;
import org.canova.api.berkeley.Triple;
import org.canova.api.writable.Writable;
import org.deeplearning4j.preprocessing.api.TransformProcess;
import org.deeplearning4j.preprocessing.api.analysis.DataAnalysis;
import org.deeplearning4j.preprocessing.api.schema.SequenceSchema;
import org.deeplearning4j.preprocessing.api.sequence.SplitMaxLengthSequence;
import org.deeplearning4j.preprocessing.api.sequence.comparator.StringComparator;
import org.deeplearning4j.preprocessing.spark.SparkTransformExecutor;
import org.deeplearning4j.preprocessing.api.filter.FilterInvalidValues;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.preprocessing.api.transform.ConditionalTransform;
import org.deeplearning4j.preprocessing.api.transform.categorical.CategoricalToIntegerTransform;
import org.deeplearning4j.preprocessing.api.transform.categorical.IntegerToCategoricalTransform;
import org.deeplearning4j.preprocessing.api.transform.categorical.StringToCategoricalTransform;
import org.deeplearning4j.preprocessing.api.transform.integer.ReplaceEmptyIntegerWithValueTransform;
import org.deeplearning4j.preprocessing.api.transform.integer.ReplaceInvalidWithIntegerTransform;
import org.deeplearning4j.preprocessing.api.transform.normalize.Normalize;
import org.deeplearning4j.preprocessing.api.transform.string.MapAllStringsExceptListTransform;
import org.deeplearning4j.preprocessing.api.transform.string.RemoveWhiteSpaceTransform;
import org.deeplearning4j.preprocessing.api.transform.string.ReplaceEmptyStringTransform;
import org.deeplearning4j.preprocessing.api.transform.string.StringMapTransform;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class NB15Util {

    public static final List<String> LABELS  = Arrays.asList("none", "Exploits", "Reconnaissance", "DoS",
            "Generic", "Shellcode", "Fuzzers", "Worms", "Backdoor", "Analysis");
    public static final List<String> SERVICES = Arrays.asList("-", "dns", "http", "smtp", "ftp-data", "ftp",
            "ssh", "pop3", "snmp", "ssl", "irc", "radius", "dhcp");
    public static final int LABELIDX = 66;
    public static final int NIN = 66;
    public static final int NOUT = 10;
    public static final int NORMALIDX = 0;

    public static Schema getCsvSchema(){

        Schema csvSchema = new Schema.Builder()
                .addColumnString("source ip")
                .addColumnInteger("source port")
                .addColumnString("destination ip")
                .addColumnInteger("destination port")
                .addColumnsString("transaction protocol","state")
                .addColumnDouble("total duration")
                .addColumnsInteger("source-dest bytes", "dest-source bytes", "source-dest time to live", "dest-source time to live",
                        "source packets lost", "destination packets lost")
                .addColumnString("service")
                .addColumnsDouble("source bits per second","destination bits per second")
                .addColumnsInteger("source-destination packet count", "dest-source packet count", "source TCP window adv", "dest TCP window adv")
                .addColumnsLong("source TCP base sequence num", "dest TCP base sequence num")
                .addColumnsInteger("source mean flow packet size",
                        "dest mean flow packet size", "transaction pipelined depth", "content size")
                .addColumnsDouble("source jitter ms", "dest jitter ms")
                .addColumnsString("timestamp start", "timestamp end")
                .addColumnsDouble("source interpacket arrival time", "destination interpacket arrival time", "tcp setup round trip time",
                        "tcp setup time syn syn_ack", "tcp setup time syn_ack ack")
                .addColumnsInteger("equal ips and ports", "count time to live", "count flow http methods", "is ftp login",
                        "count ftp commands", "count same service and source", "count same service and dest",
                        "count same dest", "count same source", "count same source addr dest port", "count same dest addr source port",
                        "count same source dest address")
                .addColumnString("attack category")
                .addColumnInteger("label")
                .build();

        return csvSchema;
    }


    public static TransformProcess getPreProcessingProcess(){

        TransformProcess seq = new TransformProcess.Builder(getCsvSchema())
//                .removeColumns("timestamp start", "timestamp end", //Don't need timestamps, we have duration. Can't really use IPs here.
//                        "source TCP base sequence num", "dest TCP base sequence num")       //Sequence numbers are essentially random between 0 and 4.29 billion
                .removeColumns("timestamp start", "timestamp end", "source ip", "destination ip",  //Don't need timestamps, we have duration. Can't really use IPs here.
                        "source TCP base sequence num", "dest TCP base sequence num")       //Sequence numbers are essentially random between 0 and 4.29 billion
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
                .transform(new IntegerToCategoricalTransform("label", Arrays.asList("normal", "attack")))
                .transform(new IntegerToCategoricalTransform("equal ips and ports", Arrays.asList("notEqual", "equal")))
                .transform(new IntegerToCategoricalTransform("is ftp login", Arrays.asList("not ftp", "ftp login")))

                .removeColumns("label") //leave attack category
                .build();

        return seq;
    }

    public static TransformProcess getSequencePreProcessingProcess(){

        //Set up the sequence of transforms:

        TransformProcess seq = new TransformProcess.Builder(getCsvSchema())
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
//                .convertToSequence("destination ip",new StringComparator("timestamp end"), SequenceSchema.SequenceType.TimeSeriesAperiodic)
                .convertToSequence("source ip",new StringComparator("timestamp end"))
                .splitSequence(new SplitMaxLengthSequence(1000,false))
                .removeColumns("timestamp start", "timestamp end", "source ip", "destination ip") //Don't need timestamps, except for ordering time steps within each sequence; don't need IPs (except for conversion to sequence)
                .build();

        return seq;
    }

    public static Triple<TransformProcess, Schema, JavaRDD<List<Writable>>>
            normalize(Schema schema, DataAnalysis da, JavaRDD<List<Writable>> input, SparkTransformExecutor executor) {

        TransformProcess norm = getNormalizerSequence(schema,da);
        Schema normSchema = norm.getFinalSchema();
        JavaRDD<List<Writable>> normalizedData = executor.execute(input, norm);
        return new Triple<>(norm, normSchema, normalizedData);
    }

    public static Triple<TransformProcess, Schema, JavaRDD<List<List<Writable>>>>
        normalizeSequence(Schema schema, DataAnalysis da, JavaRDD<List<List<Writable>>> input, SparkTransformExecutor executor) {

        TransformProcess norm = getNormalizerSequence(schema,da);
        Schema normSchema = norm.getFinalSchema();
        JavaRDD<List<List<Writable>>> normalizedData = executor.executeSequenceToSequence(input, norm);
        return new Triple<>(norm, normSchema, normalizedData);
    }

    private static TransformProcess getNormalizerSequence(Schema schema, DataAnalysis da){
        TransformProcess norm = new TransformProcess.Builder(schema)
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
        return norm;
    }

}
