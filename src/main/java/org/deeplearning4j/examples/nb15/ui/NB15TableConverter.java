package org.deeplearning4j.examples.nb15.ui;

import lombok.AllArgsConstructor;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**Convert the raw NB15 data to a table. Specifically, extract out the IPs, ports, service, bits per second etc.
 *
 *
 * Created by Alex on 14/03/2016.
 */
public class NB15TableConverter implements TableConverter {

    private final Schema schema;

    private final int sourceIP;
    private final int sourcePort;
    private final int destinationIP;
    private final int destinationPort;
    private final int service;

    private static final String[] header = new String[]{"Field","Value"};

    public NB15TableConverter(Schema schema){
        this.schema = schema;

        sourceIP = schema.getIndexOfColumn("source ip");
        destinationIP = schema.getIndexOfColumn("destination ip");
        sourcePort = schema.getIndexOfColumn("source port");
        destinationPort = schema.getIndexOfColumn("destination port");
        service = schema.getIndexOfColumn("service");
    }


    @Override
    public RenderableComponentTable rawDataToTable(Collection<Writable> writables) {
        List<Writable> list = (writables instanceof List ? (List<Writable>)writables : new ArrayList<>(writables));
        String title = "Title here";    //TODO

        String[][] table = new String[3][2];
        table[0][0] = "Source IP:Port";
        table[0][1] = list.get(sourceIP).toString() + " : " + list.get(sourcePort);

        table[1][0] = "Destination IP:Port";
        table[1][1] = list.get(destinationIP).toString() + " : " + list.get(destinationPort);

        table[2][0] = "Service";
        table[2][1] = list.get(sourceIP).toString();

        return new RenderableComponentTable(title,header,table);
    }
}
