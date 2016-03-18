package org.deeplearning4j.examples.data.api.transform;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.schema.Schema;
import org.deeplearning4j.examples.data.api.metadata.ColumnMetaData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**Map the values in a single column to new values.
 * For example: string -> string, or empty -> x type transforms for a single column
 */
@EqualsAndHashCode(callSuper = true)
@Data
public abstract class BaseColumnTransform extends BaseTransform {

    protected final String columnName;
    protected int columnNumber = -1;

    public BaseColumnTransform(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public void setInputSchema(Schema inputSchema){
        this.inputSchema = inputSchema;
        columnNumber = inputSchema.getIndexOfColumn(columnName);
    }

    @Override
    public Schema transform(Schema schema) {
        List<ColumnMetaData> oldMeta = schema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(oldMeta.size());

        Iterator<ColumnMetaData> typesIter = oldMeta.iterator();

        int i=0;
        while(typesIter.hasNext()){
            ColumnMetaData t = typesIter.next();
            if(i++ == columnNumber){
                newMeta.add(getNewColumnMetaData(t));
            } else {
                newMeta.add(t);
            }
        }

        return schema.newSchema(new ArrayList<>(schema.getColumnNames()),newMeta);
    }

    public abstract ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnType);

    @Override
    public Collection<Writable> map(Collection<Writable> writables) {
        int n = writables.size();
        List<Writable> out = new ArrayList<>(n);

        int i=0;
        for(Writable w : writables){
            if(i++ == columnNumber){
                Writable newW = map(w);
                out.add(newW);
            } else {
                out.add(w);
            }
        }

        return out;
    }



    public abstract Writable map(Writable columnWritable);

}
