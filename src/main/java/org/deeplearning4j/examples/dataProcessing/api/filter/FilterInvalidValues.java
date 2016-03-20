package org.deeplearning4j.examples.dataProcessing.api.filter;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.metadata.ColumnMetaData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * FilterInvalidValues: a filter operation that removes any examples (or sequences) if the examples/sequences contains
 * invalid values in any of a specified set of columns.
 * Invalid values are determined with respect to the schema
 */
public class FilterInvalidValues implements Filter {

    private Schema schema;
    private String[] columnsToFilterIfInvalid;
    private int[] columnIdxs;

    /**
     * @param columnsToFilterIfInvalid Columns to check for invalid values
     */
    public FilterInvalidValues(String... columnsToFilterIfInvalid) {
        if (columnsToFilterIfInvalid == null || columnsToFilterIfInvalid.length == 0)
            throw new IllegalArgumentException("Cannot filter 0/null columns");
        this.columnsToFilterIfInvalid = columnsToFilterIfInvalid;
    }

    @Override
    public void setSchema(Schema schema) {
        this.schema = schema;
        this.columnIdxs = new int[columnsToFilterIfInvalid.length];
        for (int i = 0; i < columnsToFilterIfInvalid.length; i++) {
            this.columnIdxs[i] = schema.getIndexOfColumn(columnsToFilterIfInvalid[i]);
        }
    }

    @Override
    public boolean removeExample(Collection<Writable> writables) {
        List<Writable> list = (writables instanceof List ? ((List<Writable>) writables) : new ArrayList<>(writables));

        for (int i : columnIdxs) {
            ColumnMetaData meta = schema.getMetaData(i);
            if (!meta.isValid(list.get(i))) return true; //Remove if not valid
        }
        return false;
    }

    @Override
    public boolean removeSequence(Collection<Collection<Writable>> sequence) {
        //If _any_ of the values are invalid, remove the entire sequence
        for (Collection<Writable> c : sequence) {
            if (removeExample(c)) return true;
        }
        return false;
    }
}
