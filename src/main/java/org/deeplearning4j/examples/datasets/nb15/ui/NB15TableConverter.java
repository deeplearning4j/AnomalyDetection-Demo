package org.deeplearning4j.examples.datasets.nb15.ui;

import org.canova.api.writable.Writable;
import io.skymind.echidna.api.schema.Schema;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;

import java.text.SimpleDateFormat;
import java.util.*;

/**Convert the raw NB15 data to a table. Specifically, extract out the IPs, ports, service, bits per second etc.
 *
 *
 * Created by Alex on 14/03/2016.
 */
public class NB15TableConverter implements TableConverter {

    private final Schema schema;

    private SimpleDateFormat sdf;


    private static final String[] header = new String[]{"Field","Value"};

    public NB15TableConverter(Schema schema){
        this.schema = schema;

        sdf = new SimpleDateFormat("MM/dd HH:mm:ss.SSS z");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }


    @Override
    public RenderableComponentTable rawDataToTable(Collection<Writable> writables) {
        List<Writable> list = (writables instanceof List ? (List<Writable>)writables : new ArrayList<>(writables));
        String title = "Title here";    //TODO

        String[][] table = new String[12][2];
        table[0][0] = "Source IP:Port";
        table[0][1] = list.get(schema.getIndexOfColumn("source ip")).toString() + " : " + list.get(schema.getIndexOfColumn("source port"));

        table[1][0] = "Destination IP:Port";
        table[1][1] = list.get(schema.getIndexOfColumn("destination ip")).toString() + " : " + list.get(schema.getIndexOfColumn("destination port"));

        table[2][0] = "Service";
        table[2][1] = list.get(schema.getIndexOfColumn("service")).toString();

        table[3][0] = "Start Time";
        long startTime = -1L;
        try{
            startTime = list.get(schema.getIndexOfColumn("timestamp start")).toLong();
        }catch(Exception e){ }
        table[3][1] = (startTime == -1 ? "" : sdf.format(new Date(startTime)));

        table[4][0] = "End Time";
        long endTime = -1L;
        try{
            endTime = list.get(schema.getIndexOfColumn("timestamp end")).toLong();
        }catch(Exception e){ }
        table[4][1] = (endTime == -1 ? "" : sdf.format(new Date(endTime)));

        table[5][0] = "Duration";
        table[5][1] = list.get(schema.getIndexOfColumn("total duration")) + " ms";

        table[6][0] = "Bytes Transferred (Source -> Dest)";
        table[6][1] = list.get(schema.getIndexOfColumn("source-dest bytes")).toString();

        table[7][0] = "Bytes Transferred (Dest -> Source)";
        table[7][1] = list.get(schema.getIndexOfColumn("dest-source bytes")).toString();

        table[8][0] = "HTTP Content Size";
        table[8][1] = list.get(schema.getIndexOfColumn("content size")).toString();

        table[9][0] = "Packet Count (Source -> Dest)";
        table[9][1] = list.get(schema.getIndexOfColumn("source-destination packet count")).toString();

        table[10][0] = "Packet Count (Dest -> Source)";
        table[10][1] = list.get(schema.getIndexOfColumn("dest-source packet count")).toString();

        table[11][0] = "State";
        table[11][1] = list.get(schema.getIndexOfColumn("state")).toString();

        return new RenderableComponentTable.Builder()
                .title(title).header(header).table(table)
                .border(1)
                .colWidthsPercent(40,60)
                .paddingPx(5,5,0,0)
                .backgroundColor("#FFFFFF")
                .headerColor("#CCCCCC")
                .build();
    }

    //TODO: find a better (but still general-purspose) design for this
    @Override
    public Map<String,Integer> getColumnMap(){
        Map<String,Integer> columnMap = new HashMap<>();
        columnMap.put("source-dest bytes",schema.getIndexOfColumn("source-dest bytes"));
        columnMap.put("dest-source bytes",schema.getIndexOfColumn("dest-source bytes"));
        columnMap.put("source ip",schema.getIndexOfColumn("source ip"));
        columnMap.put("destination ip",schema.getIndexOfColumn("destination ip"));
        columnMap.put("source port",schema.getIndexOfColumn("source port"));
        columnMap.put("destination port",schema.getIndexOfColumn("destination port"));
        columnMap.put("service", schema.getIndexOfColumn("service"));

        return columnMap;
    }

}
