package org.deeplearning4j.examples.data.schema;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;

import java.util.List;

/**
 * Created by Alex on 11/03/2016.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SequenceSchema extends Schema {

    public enum SequenceType {TimeSeriesPeriodic, TimeSeriesAperiodic, GeneralSequence};

    private final SequenceType sequenceType;
    private final int minSequenceLength;
    private final int maxSequenceLength;

    public SequenceSchema(List<String> columnNames, List<ColumnMetaData> columnMetaData, SequenceType sequenceType){
        this(columnNames,columnMetaData,sequenceType,0,Integer.MAX_VALUE);
    }

    public SequenceSchema(List<String> columnNames, List<ColumnMetaData> columnMetaData, SequenceType sequenceType,
                          int minSequenceLength, int maxSequenceLength) {
        super(columnNames, columnMetaData);
        this.sequenceType = sequenceType;
        this.minSequenceLength = minSequenceLength;
        this.maxSequenceLength = maxSequenceLength;
    }

    @Override
    public SequenceSchema newSchema(List<String> columnNames, List<ColumnMetaData> columnMetaData){
        return new SequenceSchema(columnNames,columnMetaData,sequenceType,minSequenceLength,maxSequenceLength);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        int nCol = numColumns();

        int maxNameLength = 0;
        for(String s :getColumnNames()){
            maxNameLength = Math.max(maxNameLength,s.length());
        }

        //Header:
        sb.append("SequenceSchema(sequenceType=").append(sequenceType).append(",minSequenceLength=")
                .append(minSequenceLength).append(",maxSequenceLength=").append(maxSequenceLength)
                .append(")\n");
        sb.append(String.format("%-6s","idx")).append(String.format("%-"+(maxNameLength+8)+"s","name"))
                .append(String.format("%-15s","type")).append("meta data").append("\n");

        for( int i=0; i<nCol; i++ ){
            String colName = getName(i);
            ColumnType type = getType(i);
            ColumnMetaData meta = getMetaData(i);
            String paddedName = String.format("%-"+(maxNameLength+8)+"s","\"" + colName + "\"");
            sb.append(String.format("%-6d",i))
                    .append(paddedName)
                    .append(String.format("%-15s",type))
                    .append(meta).append("\n");
        }

        return sb.toString();
    }

}
