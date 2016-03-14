package org.deeplearning4j.examples.nslkdd;

import org.apache.spark.api.java.JavaRDD;
import org.canova.api.berkeley.Triple;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.TransformSequence;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.executor.SparkTransformExecutor;
import org.deeplearning4j.examples.data.filter.FilterInvalidValues;
import org.deeplearning4j.examples.data.schema.Schema;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by Alex on 5/03/2016.
 */
public class NSLKDDUtil {

    public static Schema getNLSKDDCsvSchema(){

        Schema csvSchema = new Schema.Builder()
                .addColumnReal("duration")
                .addColumnsString("transaction_protocol", "service", "connection_status")
                .addColumnsReal("src_bytes", "dst_bytes")
                .addColumnInteger("land")
                .addColumnsInteger("num_wrong_fragment", "num_urgent",
                        "num_hot", "num_fail_logins", "logged_in",
                        "num_compromised", "root_shell", "su_attemped",
                        "num_root", "num_file_creations", "num_shells",
                        "num_access_files","num_outbound_cmds", "is_host_login","is_guest_login")
                .addColumnsInteger("dst_count", "srv_count")
                .addColumnsReal("serror_rate", "srv_serror_rate",
                        "rerror_rate", "srv_rerror_rate", "same_srv_rate",
                        "diff_srv_rate", "srv_diff_host_rate")
                .addColumnsInteger("dst_host_count", "dst_host_srv_count")
                .addColumnsReal("dst_host_same_srv_rate", "dst_host_diff_srv_rate",
                        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
                        "dst_host_serror_rate", "dst_host_srv_serror_rate",
                        "dst_host_rerror_rate", "dst_host_srv_rerror_rate")
                .addColumnString("attack")
                .addColumnInteger("other")
                .build();

        return csvSchema;
    }

    public static TransformSequence getpreProcessingSequence(){
        TransformSequence seq = new TransformSequence.Builder(getNLSKDDCsvSchema())
                .transform(new StringToCategoricalTransform("transaction_protocol", "udp", "tcp", "icmp"))
                .transform(new MapAllStringsExceptListTransform("service", "other",  Arrays.asList("other", "private", "http", "ftp_data","name",
                        "netbios_ns", "eco_i", "mtp", "telnet", "finger", "domain_u", "supdup", "uucp_path", "Z39_50",
                        "smtp", "csnet_ns", "uucp", "netbios_dgm", "urp_i", "auth", "domain", "ftp", "bgp", "ldap","ecr_i",
                        "gopher", "vmnet", "systat", "http_443", "efs", "whois","imap4", "iso_tsap", "echo", "klogin", "link",
                        "sunrpc", "login","kshell", "sql_net", "time", "hostnames", "exec", "ntp_u","discard", "nntp", "courier",
                        "ctf", "ssh", "daytime", "netstat", "nnsp", "IRC", "pop_3", "netbios_ssn", "remote_job","pop_2","printer",
                        "rje", "shell", "X11")))
                        // train set has urh_i=10, tim_i=8, red_i=8, pm_dump=5, tftp_u=3, http_8001=2, aol=2, harvest=2, http_2784=1
                .transform(new StringToCategoricalTransform("service", "other", "private", "http", "ftp_data","name",
                        "netbios_ns", "eco_i", "mtp", "telnet", "finger", "domain_u", "supdup", "uucp_path", "Z39_50",
                        "smtp", "csnet_ns", "uucp", "netbios_dgm", "urp_i", "auth", "domain", "ftp", "bgp", "ldap","ecr_i",
                        "gopher", "vmnet", "systat", "http_443", "efs", "whois","imap4", "iso_tsap", "echo", "klogin", "link",
                        "sunrpc", "login","kshell", "sql_net", "time", "hostnames", "exec", "ntp_u","discard", "nntp", "courier",
                        "ctf", "ssh", "daytime", "netstat", "nnsp", "IRC", "pop_3", "netbios_ssn", "remote_job","pop_2","printer",
                        "rje", "shell", "X11"))
                .transform(new StringToCategoricalTransform("connection_status", "SF", "S0", "REJ", "RSTR", "SH", "RSTO",
                        "S1", "RSTOS0", "S3", "S2", "OTH")) // state
                .transform(new StringToCategoricalTransform("attack", "normal", "neptune", "warezclient", "ipsweep", "portsweep",
                        "teardrop", "nmap", "satan", "smurf", "pod", "back", "guess_passwd", "ftp_write", "multihop", "rootkit",
                        "buffer_overflow", "imap", "warezmaster", "phf", "land", "loadmodule", "spy", "perl", "processtable",
                        "named", "xterm", "httptunnel", "saint", "ps", "mailbomb", "sendmail", "snmpgetattack", "apache2",
                        "xlock", "xsnoop", "udpstorm", "snmpguess", "worm", "sqlattack", "mscan"))
                .removeColumns("num_outbound_cmds", "other") // num_outbound_commands = 0 only not informative
                .build();

        return seq;
    }

    public static Triple<TransformSequence, Schema, JavaRDD<Collection<Writable>>>
            normalize(Schema schema, DataAnalysis da, JavaRDD<Collection<Writable>> input, SparkTransformExecutor executor) {

        TransformSequence norm = getNormalizerSequence(schema,da);
        Schema normSchema = norm.getFinalSchema();
        JavaRDD<Collection<Writable>> normalizedData = executor.execute(input, norm);
        return new Triple<>(norm, normSchema, normalizedData);
    }

    public static Triple<TransformSequence, Schema, JavaRDD<Collection<Collection<Writable>>>>
        normalizeSequence(Schema schema, DataAnalysis da, JavaRDD<Collection<Collection<Writable>>> input, SparkTransformExecutor executor) {

        TransformSequence norm = getNormalizerSequence(schema,da);
        Schema normSchema = norm.getFinalSchema();
        JavaRDD<Collection<Collection<Writable>>> normalizedData = executor.executeSequenceToSequence(input, norm);
        return new Triple<>(norm, normSchema, normalizedData);
    }

    private static TransformSequence getNormalizerSequence(Schema schema, DataAnalysis da){
        // TODO finish normalizing
        TransformSequence norm = new TransformSequence.Builder(schema)
                .normalize("duration", Normalize.Log2Mean, da) // 100K -> 0

                .normalize("dst_bytes", Normalize.Log2Mean, da)
                .normalize("src_bytes", Normalize.Log2Mean, da)

                .normalize("num_wrong_fragment", Normalize.Log2Mean, da) // 111K -> 0
                .normalize("num_urgent", Normalize.Log2Mean, da) // 111K -> 0
                .normalize("num_hot", Normalize.Log2Mean, da) // 110K -> 0
                .normalize("num_fail_logins", Normalize.Log2Mean, da) // 110K -> 0

                .normalize("num_compromised", Normalize.Log2Mean, da) // 110K -> 0

                .normalize("num_root", Normalize.Log2Mean, da) // 111K -> 0
                .normalize("num_file_creations", Normalize.Log2Mean, da) // 111K -> 0
                .normalize("num_shells", Normalize.Log2Mean, da) // 111K -> 0
                .normalize("num_access_files", Normalize.Log2Mean, da) // 111K -> 0

                .normalize("dst_count", Normalize.MinMax, da)
                .normalize("srv_count", Normalize.MinMax, da)
                .normalize("dst_host_count", Normalize.MinMax, da) // 0 -> 255
                .normalize("dst_host_srv_count", Normalize.MinMax, da) // 0 -> 255
                .normalize("dst_host_same_srv_rate", Normalize.MinMax, da) // 0 -> 255
                .normalize("dst_host_diff_srv_rate", Normalize.MinMax, da) // 0 -> 255
                .normalize("dst_host_same_src_port_rate", Normalize.MinMax, da)
                .normalize("dst_host_srv_diff_host_rate", Normalize.Log2Mean, da) // 77K -> 0
                .normalize("dst_host_serror_rate", Normalize.Log2Mean, da) // 74K -> 0
                .normalize("dst_host_srv_serror_rate", Normalize.Log2Mean, da) // 78K -> 0
                .normalize("dst_host_rerror_rate", Normalize.Log2Mean, da) // 87K -> 0
                .normalize("dst_host_srv_rerror_rate", Normalize.Log2Mean, da) // 91K ->
                .categoricalToOneHot("transaction_protocol", "service", "connection_status")
                .transform(new CategoricalToIntegerTransform("attack"))
                .build();
        return norm;
    }

}
