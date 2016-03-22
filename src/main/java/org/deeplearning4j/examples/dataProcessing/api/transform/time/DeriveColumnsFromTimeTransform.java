package org.deeplearning4j.examples.dataProcessing.api.transform.time;

import lombok.AllArgsConstructor;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.Transform;
import org.deeplearning4j.examples.dataProcessing.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.IntegerMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.StringMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.TimeMetaData;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Create a number of new columns by deriving their values from a Time column.
 * Can be used for example to create new columns with the year, month, day, hour, minute, second etc.
 *
 * @author Alex Black
 */
public class DeriveColumnsFromTimeTransform implements Transform {

    private final String columnName;
    private final String insertAfter;
    private final List<DerivedColumn> derivedColumns;
    private Schema inputSchema;
    private DateTimeZone inputTimeZone;
    private int insertAfterIdx = -1;
    private int deriveFromIdx = -1;


    private DeriveColumnsFromTimeTransform(Builder builder) {
        this.derivedColumns = builder.derivedColumns;
        this.columnName = builder.columnName;
        this.insertAfter = builder.insertAfter;
    }

    @Override
    public Schema transform(Schema inputSchema) {
        List<ColumnMetaData> oldMeta = inputSchema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(oldMeta.size() + derivedColumns.size());

        List<String> oldNames = inputSchema.getColumnNames();
        List<String> newNames = new ArrayList<>(oldNames.size() + derivedColumns.size());

        for (int i = 0; i < oldMeta.size(); i++) {
            String current = oldNames.get(i);
            newNames.add(current);
            newMeta.add(oldMeta.get(i));

            if (insertAfter.equals(current)) {
                //Insert the derived columns here
                for (DerivedColumn d : derivedColumns) {
                    newNames.add(d.columnName);
                    switch (d.columnType) {
                        case String:
                            newMeta.add(new StringMetaData());
                            break;
                        case Integer:
                            newMeta.add(new IntegerMetaData());     //TODO: ranges... if it's a day, we know it must be 1 to 31, etc...
                            break;
                        default:
                            throw new IllegalStateException("Unexpected column type: " + d.columnType);
                    }
                }
            }
        }

        return inputSchema.newSchema(newNames, newMeta);
    }

    @Override
    public void setInputSchema(Schema inputSchema) {
        insertAfterIdx = inputSchema.getColumnNames().indexOf(insertAfter);
        if (insertAfterIdx == -1) {
            throw new IllegalStateException("Invalid schema/insert after column: input schema does not contain column \"" + insertAfter + "\"");
        }

        deriveFromIdx = inputSchema.getColumnNames().indexOf(columnName);
        if (deriveFromIdx == -1) {
            throw new IllegalStateException("Invalid source column: input schema does not contain column \"" + columnName + "\"");
        }

        this.inputSchema = inputSchema;

        if (!(inputSchema.getMetaData(columnName) instanceof TimeMetaData))
            throw new IllegalStateException("Invalid state: input column \"" +
                    columnName + "\" is not a time column. Is: " + inputSchema.getMetaData(columnName));
        TimeMetaData meta = (TimeMetaData) inputSchema.getMetaData(columnName);
        inputTimeZone = meta.getTimeZone();
    }

    @Override
    public List<Writable> map(List<Writable> writables) {
        int i = 0;
        Writable source = writables.get(deriveFromIdx);
        List<Writable> list = new ArrayList<>(writables.size() + derivedColumns.size());
        for (Writable w : writables) {
            list.add(w);
            if (i == insertAfterIdx) {
                for (DerivedColumn d : derivedColumns) {
                    switch (d.columnType) {
                        case String:
                            list.add(new Text(d.formatter.print(source.toLong())));
                            break;
                        case Integer:
                            DateTime dt = new DateTime(source.toLong(), inputTimeZone);
                            list.add(new IntWritable(dt.get(d.fieldType)));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected column type: " + d.columnType);
                    }
                }
            }
        }
        return list;
    }

    @Override
    public List<List<Writable>> mapSequence(List<List<Writable>> sequence) {
        List<List<Writable>> out = new ArrayList<>(sequence.size());
        for (List<Writable> step : sequence) {
            out.add(map(step));
        }
        return out;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("DeriveColumnsFromTimeTransform(timeColumnName=").append(columnName)
                .append(",insertAfter=").append(insertAfter).append("derivedColumns=(");

        boolean first = true;
        for(DerivedColumn d : derivedColumns){
            if(!first) sb.append(",");
            sb.append(d);
            first = false;
        }

        sb.append("))");

        return sb.toString();
    }

    public class Builder {

        private final String columnName;
        private String insertAfter;
        private final List<DerivedColumn> derivedColumns = new ArrayList<>();


        /**
         * @param timeColumnName The name of the time column from which to derive the new values
         */
        public Builder(String timeColumnName) {
            this.columnName = timeColumnName;
            this.insertAfter = timeColumnName;
        }

        /**
         * Where should the new columns be inserted?
         * By default, they will be insterted after the source column
         *
         * @param columnName Name of the column to insert the derived columns after
         * @return
         */
        public Builder insertAfter(String columnName) {
            this.insertAfter = columnName;
            return this;
        }

        public Builder addStringDerivedColumn(String columnName, DateTimeFormatter formatter) {
            derivedColumns.add(new DerivedColumn(columnName, ColumnType.String, formatter, null));
            return this;
        }

        public Builder addIntegerDerivedColumn(String columnName, DateTimeFieldType type) {
            derivedColumns.add(new DerivedColumn(columnName, ColumnType.Integer, null, type));
            return this;
        }

        public DeriveColumnsFromTimeTransform build() {
            return new DeriveColumnsFromTimeTransform(this);
        }
    }

    @AllArgsConstructor
    private static class DerivedColumn implements Serializable {
        private final String columnName;
        private final ColumnType columnType;
        private final DateTimeFormatter formatter;
        private final DateTimeFieldType fieldType;

        @Override
        public String toString(){
            return "(name=" + columnName + ",type=" + columnType + ",derived=" + (formatter != null ? formatter : fieldType ) + ")";
        }
    }
}
