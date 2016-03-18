package org.deeplearning4j.examples.dataProcessing.api.schema;

import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.metadata.*;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Alex on 4/03/2016.
 */
public class Schema implements Serializable {

    private List<String> columnNames;
    private List<ColumnMetaData> columnMetaData;
    private Map<String,Integer> columnNamesIndex;   //For efficient lookup


    private Schema(Builder builder){
        this.columnNames = builder.columnNames;
        this.columnMetaData = builder.columnMetaData;
        columnNamesIndex = new HashMap<>();
        for(int i=0; i<columnNames.size(); i++ ){
            columnNamesIndex.put(columnNames.get(i),i);
        }
    }

    protected Schema(List<String> columnNames, List<ColumnMetaData> columnMetaData){
        if(columnNames == null || columnMetaData == null) throw new IllegalArgumentException("Input cannot be null");
        if(columnNames.size() == 0 || columnNames.size() != columnMetaData.size()) throw new IllegalArgumentException("List sizes must match (and be non-zero)");
        this.columnNames = columnNames;
        this.columnMetaData = columnMetaData;
        this.columnNamesIndex = new HashMap<>();
        for(int i=0; i<columnNames.size(); i++ ){
            columnNamesIndex.put(columnNames.get(i),i);
        }
    }

    public Schema newSchema(List<String> columnNames, List<ColumnMetaData> columnMetaData){
        return new Schema(columnNames,columnMetaData);
    }

    public int numColumns(){
        return columnNames.size();
    }

    public String getName(int column){
        return columnNames.get(column);
    }

    public ColumnType getType(int column){
        return columnMetaData.get(column).getColumnType();
    }

    public ColumnMetaData getMetaData(int column){
        return columnMetaData.get(column);
    }

    public ColumnMetaData getMetaData(String column){
        return getMetaData(getIndexOfColumn(column));
    }

    public List<String> getColumnNames(){
        return new ArrayList<>(columnNames);
    }

    public List<ColumnType> getColumnTypes(){
        List<ColumnType> list = new ArrayList<>(columnMetaData.size());
        for(ColumnMetaData md : columnMetaData) list.add(md.getColumnType());
        return list;
    }

    public List<ColumnMetaData> getColumnMetaData(){
        return new ArrayList<>(columnMetaData);
    }

    public int getIndexOfColumn(String columnName){
        Integer idx = columnNamesIndex.get(columnName);
        if(idx == null) throw new NoSuchElementException("Unknown column: \"" + columnName + "\"");
        return idx;
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
        sb.append("Schema():\n");
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

    public static class Builder {

        List<String> columnNames = new ArrayList<>();
        List<ColumnMetaData> columnMetaData = new ArrayList<>();

        public Builder addColumnString(String name){
            return addColumn(name,new StringMetaData());
        }

        public Builder addColumnReal(String name){
            return addColumn(name,new DoubleMetaData());
        }

        public Builder addColumnsReal(String... columnNames){
            for(String s : columnNames) addColumnReal(s);
            return this;
        }

        public Builder addColumnInteger(String name){
            return addColumn(name,new IntegerMetaData());
        }

        public Builder addColumnsInteger(String... names){
            for(String s : names) addColumnInteger(s);
            return this;
        }

        public Builder addColumnCategorical(String name, String... stateNames){
            return addColumn(name,new CategoricalMetaData(stateNames));
        }

        public Builder addColumnCategorical(String name, List<String> stateNames){
            return addColumn(name,new CategoricalMetaData(stateNames));
        }

        public Builder addColumnLong(String name){
            return addColumn(name, new LongMetaData());
        }

        public Builder addColumnsLong(String... names){
            for(String s : names) addColumnLong(s);
            return this;
        }


        public Builder addColumn(String name, ColumnMetaData metaData){
            columnNames.add(name);
            columnMetaData.add(metaData);
            return this;
        }

        public Builder addColumnsString(String... columnNames){
            for(String s : columnNames) addColumnString(s);
            return this;
        }



        public Schema build(){
            return new Schema(this);
        }
    }

}
