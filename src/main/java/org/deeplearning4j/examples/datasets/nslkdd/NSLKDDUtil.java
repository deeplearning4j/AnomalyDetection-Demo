package org.deeplearning4j.examples.datasets.nslkdd;

import org.apache.spark.api.java.JavaRDD;
import org.canova.api.berkeley.Triple;
import org.canova.api.writable.Writable;
import io.skymind.echidna.api.TransformProcess;
import io.skymind.echidna.api.analysis.DataAnalysis;
import io.skymind.echidna.spark.SparkTransformExecutor;
import io.skymind.echidna.api.schema.Schema;
import io.skymind.echidna.api.transform.categorical.CategoricalToIntegerTransform;
import io.skymind.echidna.api.transform.categorical.StringToCategoricalTransform;
import io.skymind.echidna.api.transform.normalize.Normalize;
import io.skymind.echidna.api.transform.string.MapAllStringsExceptListTransform;

import java.util.Arrays;
import java.util.List;

/**
 *
 * References:
 *  http://www.ijarcce.com/upload/2015/june-15/IJARCCE%2096.pdf
 *  http://citeseerx.ist.psu.edu/viewdoc/download;jsessionid=7C05CDF892E875A01EDF75C2970CBDB9?doi=10.1.1.680.6760&rep=rep1&type=pdf
 */
public class NSLKDDUtil {


    public static final List<String> LABELS = Arrays.asList("normal", "neptune", "warezclient", "ipsweep", "portsweep",
            "teardrop", "nmap", "satan", "smurf", "pod", "back", "guess_passwd", "ftp_write", "multihop", "rootkit",
            "buffer_overflow", "imap", "warezmaster", "phf", "land", "loadmodule", "spy", "perl", "processtable",
            "named", "xterm", "httptunnel", "saint", "ps", "mailbomb", "sendmail", "snmpgetattack", "apache2",
            "xlock", "xsnoop", "udpstorm", "snmpguess", "worm", "sqlattack", "mscan");
    public static final List<String> SERVICES = Arrays.asList("other", "private", "http", "ftp_data","name",
            "netbios_ns", "eco_i", "mtp", "telnet", "finger", "domain_u", "supdup", "uucp_path", "Z39_50",
            "smtp", "csnet_ns", "uucp", "netbios_dgm", "urp_i", "auth", "domain", "ftp", "bgp", "ldap","ecr_i",
            "gopher", "vmnet", "systat", "http_443", "efs", "whois","imap4", "iso_tsap", "echo", "klogin", "link",
            "sunrpc", "login","kshell", "sql_net", "time", "hostnames", "exec", "ntp_u","discard", "nntp", "courier",
            "ctf", "ssh", "daytime", "netstat", "nnsp", "IRC", "pop_3", "netbios_ssn", "remote_job","pop_2","printer",
            "rje", "shell", "X11");
    public static final int LABELIDX = 112;
    public static final int NIN = 112;
    public static final int NOUT = 40;
    public static final int NORMALIDX = 0; // TODO find where this is?

    public static Schema getCsvSchema(){

        Schema csvSchema = new Schema.Builder()
                .addColumnDouble("duration")
                .addColumnCategorical("transaction_protocol", "udp", "tcp", "icmp")
                .addColumnString("service")
                .addColumnCategorical("connection_status", "SF", "S0", "REJ", "RSTR", "SH", "RSTO",
                        "S1", "RSTOS0", "S3", "S2", "OTH")
                .addColumnsDouble("src_bytes", "dst_bytes")
                .addColumnInteger("land")
                .addColumnsInteger("num_wrong_fragment", "num_urgent",
                        "num_hot", "num_fail_logins", "logged_in",
                        "num_compromised", "root_shell", "su_attemped",
                        "num_root", "num_file_creations", "num_shells",
                        "num_access_files","num_outbound_cmds", "is_host_login","is_guest_login")
                .addColumnsInteger("dst_count", "srv_count")
                .addColumnsDouble("serror_rate", "srv_serror_rate",
                        "rerror_rate", "srv_rerror_rate", "same_srv_rate",
                        "diff_srv_rate", "srv_diff_host_rate")
                .addColumnsInteger("dst_host_count", "dst_host_srv_count")
                .addColumnsDouble("dst_host_same_srv_rate", "dst_host_diff_srv_rate",
                        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
                        "dst_host_serror_rate", "dst_host_srv_serror_rate",
                        "dst_host_rerror_rate", "dst_host_srv_rerror_rate")
                .addColumnCategorical("attack", LABELS)
                .addColumnInteger("other")
                .build();

        return csvSchema;
    }

    // Supervised
    public static TransformProcess getPreProcessingProcess(){
        TransformProcess seq = new TransformProcess.Builder(getCsvSchema())
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
                .removeColumns("num_outbound_cmds", "other") // num_outbound_commands = 0 only not informative
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
        // TODO finish normalizing
        TransformProcess norm = new TransformProcess.Builder(schema)
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
