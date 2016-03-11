package org.deeplearning4j.examples.data.filter;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Filter;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class FilterInvalidValues implements Filter {

    private Schema schema;
    private String[] columnsToFilterIfInvalid;
    private int[] columnIdxs;

    public FilterInvalidValues(String... columnsToFilterIfInvalid){
        if(columnsToFilterIfInvalid == null || columnsToFilterIfInvalid.length == 0) throw new IllegalArgumentException("Cannot filter 0/null columns");
        this.columnsToFilterIfInvalid = columnsToFilterIfInvalid;
    }

    @Override
    public void setSchema(Schema schema){
        this.schema = schema;
        this.columnIdxs = new int[columnsToFilterIfInvalid.length];
        for( int i=0; i<columnsToFilterIfInvalid.length; i++ ){
            this.columnIdxs[i] = schema.getIndexOfColumn(columnsToFilterIfInvalid[i]);
        }
    }

    @Override
    public boolean removeExample(Collection<Writable> writables) {
        List<Writable> list = (writables instanceof List ? ((List<Writable>)writables) : new ArrayList<>(writables));

        for( int i : columnIdxs){
            ColumnMetaData meta = schema.getMetaData(i);
            if(!meta.isValid(list.get(i))) return true; //Remove if not valid
        }
        return false;
    }

    @Override
    public boolean removeSequence(Collection<Collection<Writable>> sequence) {
        //If _any_ of the values are invalid, remove the entire sequence
        for(Collection<Writable> c : sequence ){
            if(removeExample(c)) return true;
        }
        return false;
    }
}
