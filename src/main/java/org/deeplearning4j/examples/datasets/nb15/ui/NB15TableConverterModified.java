package org.deeplearning4j.examples.datasets.nb15.ui;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.ui.UIDriverModified;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;
import org.deeplearning4j.preprocessing.api.schema.Schema;

import java.text.SimpleDateFormat;
import java.util.*;

/**Convert the raw NB15 data to a table. Specifically, extract out the IPs, ports, service, bits per second etc.
 *
 *
 * Created by Alex on 14/03/2016.
 */
public class NB15TableConverterModified implements TableConverter {

    private final Schema schema;

    private SimpleDateFormat sdf;

    private static final Random r = new Random(12345);


    private static final String[] header = new String[]{"Field","Value"};

    private static final String[] levels = new String[]{"INFO","WARN","ERROR","FATAL","Other","None","Custom0","Custom1","Custom2","Custom3","Custom4"};

    public NB15TableConverterModified(Schema schema){
        this.schema = schema;

        sdf = new SimpleDateFormat("MM/dd HH:mm:ss.SSS z");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }


    @Override
    public RenderableComponentTable rawDataToTable(Collection<Writable> writables) {
        List<Writable> list = (writables instanceof List ? (List<Writable>)writables : new ArrayList<>(writables));
        String title = "Title here";    //TODO

        String[][] table = new String[5][2];
        table[0][0] = "Machine IP";
        table[0][1] = list.get(schema.getIndexOfColumn("source ip")).toString();


        table[1][0] = "Service";
        table[1][1] = "-";

        table[2][0] = "Log Entry Time";
        long startTime = -1L;
        try{
            startTime = list.get(schema.getIndexOfColumn("timestamp start")).toLong();
        }catch(Exception e){ }
        table[2][1] = (startTime == -1 ? "" : sdf.format(new Date(startTime)));

        table[3][0] = "Log Entry Size";
        table[3][1] = r.nextInt(401) + " characters";

        table[4][0] = "Log Level";
        double d = r.nextDouble();
        if(d <= 0.9){
            //90% probability of generating from first 6
            table[4][1] = levels[r.nextInt(6)];
        } else {
            //10% probability of generating from all 11
            table[4][1] = levels[r.nextInt(levels.length)];
        }


//        table[4][0] = "End Time";
//        long endTime = -1L;
//        try{
//            endTime = list.get(schema.getIndexOfColumn("timestamp end")).toLong();
//        }catch(Exception e){ }
//        table[4][1] = (endTime == -1 ? "" : sdf.format(new Date(endTime)));
//
//        table[5][0] = "Duration";
//        table[5][1] = list.get(schema.getIndexOfColumn("total duration")) + " ms";
//
//        table[6][0] = "Bytes Transferred (Source -> Dest)";
//        table[6][1] = list.get(schema.getIndexOfColumn("source-dest bytes")).toString();
//
//        table[7][0] = "Bytes Transferred (Dest -> Source)";
//        table[7][1] = list.get(schema.getIndexOfColumn("dest-source bytes")).toString();
//
//        table[8][0] = "HTTP Content Size";
//        table[8][1] = list.get(schema.getIndexOfColumn("content size")).toString();
//
//        table[9][0] = "Packet Count (Source -> Dest)";
//        table[9][1] = list.get(schema.getIndexOfColumn("source-destination packet count")).toString();
//
//        table[10][0] = "Packet Count (Dest -> Source)";
//        table[10][1] = list.get(schema.getIndexOfColumn("dest-source packet count")).toString();
//
//        table[11][0] = "State";
//        table[11][1] = list.get(schema.getIndexOfColumn("state")).toString();

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
