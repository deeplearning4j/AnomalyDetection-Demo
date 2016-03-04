package org.deeplearning4j.examples.data.transform;

import lombok.Data;
import org.apache.commons.lang3.ArrayUtils;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.Transform;

import java.io.Serializable;
import java.util.*;

@Data
public class RemoveColumnsTransform implements Transform, Serializable {

    private int[] columnsToRemove;
    private String[] columnsToRemoveByName;
    private Set<Integer> indicesToRemove;

    public RemoveColumnsTransform(int... columnsToRemove){
        this.columnsToRemove = columnsToRemove;
    }

    public RemoveColumnsTransform(String... columnsToRemove){
        this.columnsToRemoveByName = columnsToRemove;
    }

    public Set<Integer> getIndicesToRemove(){
        if(indicesToRemove == null){
            indicesToRemove = new HashSet<>();
            for( int i : columnsToRemove ) indicesToRemove.add(i);
        }
        return indicesToRemove;
    }

    @Override
    public Schema transform(Schema schema) {
        int nToRemove = (columnsToRemove != null ? columnsToRemove.length : columnsToRemoveByName.length);
        int newNumColumns = schema.numColumns() - nToRemove;
        if(newNumColumns <= 0 ) throw new IllegalStateException();

        List<String> origNames = schema.getColumnNames();
        List<ColumnType> origTypes = schema.getColumnTypes();

        if(columnsToRemoveByName == null){
            columnsToRemoveByName = new String[columnsToRemove.length];
            for( int i=0; i<columnsToRemove.length; i++ ){
                columnsToRemoveByName[i] = origNames.get(columnsToRemove[i]);
            }
        } else {
            //Check that each column to remove actually exists...
            int i=0;
            columnsToRemove = new int[columnsToRemoveByName.length];
            String[] allNames = schema.getColumnNames().toArray(new String[schema.numColumns()]);
            for( String s : columnsToRemoveByName){
                int idx = ArrayUtils.indexOf(allNames,s);
                if(idx<0) throw new RuntimeException("Column \"" + s + "\" not found");
                columnsToRemove[i++] = idx;
            }
        }
        Set<String> set = new HashSet<>();
        Collections.addAll(set, columnsToRemoveByName);


        List<String> newNames = new ArrayList<>(newNumColumns);
        List<ColumnType> newTypes = new ArrayList<>(newNumColumns);

        Iterator<String> namesIter = origNames.iterator();
        Iterator<ColumnType> typesIter = origTypes.iterator();

        while(namesIter.hasNext()){
            String n = namesIter.next();
            ColumnType t = typesIter.next();
            if(!set.contains(n)){
                newNames.add(n);
                newTypes.add(t);
            }
        }

        return new Schema(newNames,newTypes);
    }
}
