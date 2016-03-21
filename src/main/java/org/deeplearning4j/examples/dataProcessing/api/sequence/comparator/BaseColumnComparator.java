package org.deeplearning4j.examples.dataProcessing.api.sequence.comparator;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.SequenceSchema;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceComparator;

import java.util.List;

/**Compare/sort a sequence by the values of a specific column
 * Created by Alex on 11/03/2016.
 */
public abstract class BaseColumnComparator implements SequenceComparator {

    private SequenceSchema schema;

    private final String columnName;
    private int columnIdx = -1;

    protected BaseColumnComparator(String columnName){
        this.columnName = columnName;
    }

    @Override
    public void setSchema(SequenceSchema sequenceSchema) {
        this.schema = sequenceSchema;
        this.columnIdx = sequenceSchema.getIndexOfColumn(columnName);
    }

    @Override
    public int compare(List<Writable> o1, List<Writable> o2) {
        return compare(get(o1,columnIdx),get(o2,columnIdx));
    }

    private static Writable get(List<Writable> c, int idx){
        return c.get(idx);
    }

    protected abstract int compare(Writable w1, Writable w2);
}
