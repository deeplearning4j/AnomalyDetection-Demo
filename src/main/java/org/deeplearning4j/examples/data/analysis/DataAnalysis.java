package org.deeplearning4j.examples.data.analysis;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.analysis.columns.ColumnAnalysis;

import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class DataAnalysis {

    private Schema schema;
    private List<ColumnAnalysis> columnAnalysis;

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        int nCol = schema.numColumns();

        int maxNameLength = 0;
        for(String s :schema.getColumnNames()){
            maxNameLength = Math.max(maxNameLength,s.length());
        }

        //Header:
        sb.append(String.format("%-6s","idx")).append(String.format("%-"+(maxNameLength+8)+"s","name"))
                .append(String.format("%-15s","type")).append("analysis").append("\n");

        for( int i=0; i<nCol; i++ ){
            String colName = schema.getName(i);
            ColumnType type = schema.getType(i);
            ColumnAnalysis analysis = columnAnalysis.get(i);
            String paddedName = String.format("%-"+(maxNameLength+8)+"s","\"" + colName + "\"");
            sb.append(String.format("%-6d",i))
                    .append(paddedName)
                    .append(String.format("%-15s",type))
                    .append(analysis).append("\n");
        }

        return sb.toString();
    }


}
