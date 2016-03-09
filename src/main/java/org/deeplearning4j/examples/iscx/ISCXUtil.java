package org.deeplearning4j.examples.iscx;

import org.deeplearning4j.examples.data.Schema;

/**
 * Created by Alex on 5/03/2016.
 */
public class ISCXUtil {

    public static Schema getCsvSchema(){

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

}
