package org.deeplearning4j.examples.datasets.iscx;

import org.apache.spark.api.java.JavaRDD;
import org.datavec.api.berkeley.Triple;
import org.datavec.api.writable.Writable;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.analysis.DataAnalysis;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.transform.categorical.CategoricalToIntegerTransform;
import org.datavec.api.transform.transform.categorical.StringToCategoricalTransform;
import org.datavec.api.transform.transform.normalize.Normalize;
import org.datavec.api.transform.transform.string.StringListToCategoricalSetTransform;
import org.datavec.spark.transform.SparkTransformExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public class ISCXUtil {

    // TODO expand
    public static final List<String> LABELS = Arrays.asList("none", "Exploits");

    public static Schema getCsvSchema() {

        return new Schema.Builder()
                .addColumnString("appName")
                .addColumnInteger("totalSourceBytes")
                .addColumnInteger("totalDestinationBytes")
                .addColumnInteger("totalDestinationPackets")
                .addColumnsInteger("total source packets")
                .addColumnsString("source payload base64", "destination payload base64")
                .addColumnString("direction")
                .addColumnsString("source TCP flags")
                .addColumnsString("destination TCP flags")
                .addColumnString("source ip")
                .addColumnString("protocol name")
                .addColumnString("destination ip")
                .addColumnInteger("destination port")
                .addColumnString("start time")
                .addColumnString("end time")
                .addColumnString("label")
                .build();
    }

    public static TransformProcess getPreProcessingProcess() {
        return new TransformProcess.Builder(getCsvSchema())
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
                .build();
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

        return new TransformProcess.Builder(schema)
                .normalize("totalSourceBytes", Normalize.Log2Mean, da)
                .normalize("totalDestinationBytes", Normalize.Log2Mean, da)
                .normalize("totalDestinationPackets", Normalize.Log2Mean, da)
                //Do conversion of categorical fields to a set of one-hot columns, ready for network training:
                .categoricalToOneHot("direction", "protocol name", "appName", "sourceTCP_F", "destinationTCP_F")
                .transform(new CategoricalToIntegerTransform("label"))
                .build();


    }


}
