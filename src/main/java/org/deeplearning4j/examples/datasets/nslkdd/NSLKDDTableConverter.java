package org.deeplearning4j.examples.datasets.nslkdd;

import org.canova.api.writable.Writable;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;

import java.text.SimpleDateFormat;
import java.util.*;

/**Convert raw data to a table.
 */

public class NSLKDDTableConverter implements TableConverter {

    private final Schema schema;

//    private SimpleDateFormat sdf;


    private static final String[] header = new String[]{"Field","Value"};

    public NSLKDDTableConverter(Schema schema){
        this.schema = schema;

//        sdf = new SimpleDateFormat("MM/dd HH:mm:ss.SSS z");
//        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }


    @Override
    public RenderableComponentTable rawDataToTable(Collection<Writable> writables) {
        List<Writable> list = (writables instanceof List ? (List<Writable>)writables : new ArrayList<>(writables));
        String title = "Network Intrusion Detection";

        String[][] table = new String[9][2];
        table[0][0] = "Successfully Logged in";
        table[0][1] = list.get(schema.getIndexOfColumn("logged_in")).toString();

        table[1][0] = "Connection Protocol";
        table[1][1] = list.get(schema.getIndexOfColumn("transaction_protocol")).toString();

        table[2][0] = "Service";
        table[2][1] = list.get(schema.getIndexOfColumn("service")).toString();

        table[3][0] = "Duration";
        table[3][1] = list.get(schema.getIndexOfColumn("duration")) + " ms";

        table[4][0] = "Bytes Transferred (Source -> Dest)";
        table[4][1] = list.get(schema.getIndexOfColumn("src_bytes")).toString();

        table[5][0] = "Bytes Transferred (Dest -> Source)";
        table[5][1] = list.get(schema.getIndexOfColumn("dst_bytes")).toString();

        table[6][0] = "Connections Same Service (past 2 secs)";
        table[6][1] = list.get(schema.getIndexOfColumn("srv_count")).toString();

        table[7][0] = "Connections Dame Destination (past 2 secs)";
        table[7][1] = list.get(schema.getIndexOfColumn("dst_count")).toString();

        table[8][0] = "State";
        table[8][1] = list.get(schema.getIndexOfColumn("connection_status")).toString();

        return new RenderableComponentTable.Builder()
                .title(title).header(header).table(table)
                .border(1)
                .colWidthsPercent(40,60)
                .paddingPx(5,5,0,0)
                .backgroundColor("#FFFFFF")
                .headerColor("#CCCCCC")
                .build();
    }

    @Override
    public Map<String,Integer> getColumnMap(){
        Map<String,Integer> columnMap = new HashMap<>();
        columnMap.put("logged_in",schema.getIndexOfColumn("logged_in"));
        columnMap.put("transaction_protocol",schema.getIndexOfColumn("transaction_protocol"));
        columnMap.put("service", schema.getIndexOfColumn("service"));
        columnMap.put("duration",schema.getIndexOfColumn("duration"));
        columnMap.put("source-dest bytes",schema.getIndexOfColumn("src_bytes"));
        columnMap.put("dest-source bytes",schema.getIndexOfColumn("dst_bytes"));
        columnMap.put("srv_count",schema.getIndexOfColumn("srv_count"));
        columnMap.put("dst_count",schema.getIndexOfColumn("dst_count"));
        columnMap.put("state",schema.getIndexOfColumn("connection_status"));
        return columnMap;
    }

}
