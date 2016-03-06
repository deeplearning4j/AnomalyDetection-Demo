package org.deeplearning4j.examples.nb15;

import org.deeplearning4j.examples.data.Schema;

/**
 * Created by Alex on 5/03/2016.
 */
public class NB15Util {

    public static Schema getNB15CsvSchema(){

        Schema csvSchema = new Schema.Builder()
                .addColumnString("source ip")
                .addColumnInteger("source port")
                .addColumnString("destination ip")
                .addColumnInteger("destination port")
                .addColumnsString("transaction protocol","state")
                .addColumnReal("total duration")
                .addColumnsInteger("source-dest bytes", "dest-source bytes", "source-dest time to live", "dest-source time to live",
                        "source packets lost", "destination packets lost")
                .addColumnString("service")
                .addColumnsReal("source bits per second","destination bits per second")
                .addColumnsInteger("source-destination packet count", "dest-source packet count", "source TCP window adv", "dest TCP window adv")
                .addColumnsLong("source TCP base sequence num", "dest TCP base sequence num")
                .addColumnsInteger("source mean flow packet size",
                        "dest mean flow packet size", "transaction pipelined depth", "content size")
                .addColumnsReal("source jitter ms", "dest jitter ms")
                .addColumnsString("timestamp start", "timestamp end")
                .addColumnsReal("source interpacket arrival time", "destination interpacket arrival time", "tcp setup round trip time",
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

}
