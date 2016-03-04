package org.deeplearning4j.examples.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class Schema {

    private List<String> columnNames;
    private List<ColumnType> columnTypes;


    private Schema(Builder builder){
        this.columnNames = builder.columnNames;
        this.columnTypes = builder.columnTypes;
    }

    public Schema(List<String> columnNames, List<ColumnType> columnTypes){
        if(columnNames == null || columnTypes == null) throw new IllegalArgumentException("Input cannot be null");
        if(columnNames.size() == 0 || columnNames.size() != columnTypes.size()) throw new IllegalArgumentException("List sizes must match (and be non-zero)");
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public int numColumns(){
        return columnNames.size();
    }

    public String getName(int column){
        return columnNames.get(column);
    }

    public ColumnType getType(int column){
        return columnTypes.get(column);
    }

    public List<String> getColumnNames(){
        return new ArrayList<>(columnNames);
    }

    public List<ColumnType> getColumnTypes(){
        return new ArrayList<>(columnTypes);
    }

    public static class Builder {

        List<String> columnNames = new ArrayList<>();
        List<ColumnType> columnTypes = new ArrayList<>();

        public Builder addColumnString(String name){
            return addColumn(name,ColumnType.String);
        }

        public Builder addColumnReal(String name){
            return addColumn(name,ColumnType.Double);
        }

        public Builder addColumnsReal(String... columnNames){
            for(String s : columnNames) addColumnReal(s);
            return this;
        }

        public Builder addColumnInteger(String name){
            return addColumn(name,ColumnType.Integer);
        }

        public Builder addColumnsInteger(String... names){
            for(String s : names) addColumnInteger(s);
            return this;
        }

        public Builder addColumnCategorical(String name){
            return addColumn(name,ColumnType.Categorical);
        }

        public Builder addColumnsCategorical(String... names){
            for(String s : names) addColumnCategorical(s);
            return this;
        }


        public Builder addColumn(String name, ColumnType type){
            columnNames.add(name);
            columnTypes.add(type);
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
