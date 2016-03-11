package org.deeplearning4j.examples.data.transform.column;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.transform.BaseTransform;

import java.util.*;

@EqualsAndHashCode(callSuper = true)
@Data
public class RemoveColumnsTransform extends BaseTransform {

    private int[] columnsToRemoveIdx;
    private String[] columnsToRemove;
    private Set<Integer> indicesToRemove;

    public RemoveColumnsTransform(String... columnsToRemove){
        this.columnsToRemove = columnsToRemove;
    }

//    public Set<Integer> getIndicesToRemove(){
//        if(indicesToRemove == null){
//            indicesToRemove = new HashSet<>();
//            for( int i : columnsToRemoveIdx) indicesToRemove.add(i);
//        }
//        return indicesToRemove;
//    }

    @Override
    public void setInputSchema(Schema schema){
        super.setInputSchema(schema);

        indicesToRemove = new HashSet<>();

        int i=0;
        columnsToRemoveIdx = new int[columnsToRemove.length];
        String[] allNames = schema.getColumnNames().toArray(new String[schema.numColumns()]);
        for( String s : columnsToRemove){
            int idx = schema.getIndexOfColumn(s);
            if(idx<0) throw new RuntimeException("Column \"" + s + "\" not found");
            columnsToRemoveIdx[i++] = idx;
            indicesToRemove.add(idx);
        }
    }

    @Override
    public Schema transform(Schema schema) {
        int nToRemove = columnsToRemove.length;
        int newNumColumns = schema.numColumns() - nToRemove;
        if(newNumColumns <= 0 ) throw new IllegalStateException();

        List<String> origNames = schema.getColumnNames();
        List<ColumnMetaData> origMeta = schema.getColumnMetaData();

        Set<String> set = new HashSet<>();
        Collections.addAll(set, columnsToRemove);


        List<String> newNames = new ArrayList<>(newNumColumns);
        List<ColumnMetaData> newMeta = new ArrayList<>(newNumColumns);

        Iterator<String> namesIter = origNames.iterator();
        Iterator<ColumnMetaData> metaIter = origMeta.iterator();

        while(namesIter.hasNext()){
            String n = namesIter.next();
            ColumnMetaData t = metaIter.next();
            if(!set.contains(n)){
                newNames.add(n);
                newMeta.add(t);
            }
        }

        return schema.newSchema(newNames,newMeta);
    }

    @Override
    public Collection<Writable> map(Collection<Writable> writables) {

        List<Writable> outList = new ArrayList<>(writables.size()-columnsToRemove.length);

        int i=0;
        for(Writable w : writables){
            if(indicesToRemove.contains(i++)) continue;
            outList.add(w);
        }
        return outList;
    }
}
