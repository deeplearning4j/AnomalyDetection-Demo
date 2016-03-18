package org.deeplearning4j.examples.data.api.sequence.comparator;

import org.apache.commons.collections.CollectionUtils;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.schema.SequenceSchema;
import org.deeplearning4j.examples.data.api.sequence.SequenceComparator;

import java.util.Collection;
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
    public int compare(Collection<Writable> o1, Collection<Writable> o2) {
        return compare(get(o1,columnIdx),get(o2,columnIdx));
    }

    private static Writable get(Collection<Writable> c, int idx){
        if(c instanceof List) return ((List<Writable>)c).get(idx);
        return (Writable)CollectionUtils.get(c,idx);
    }

    protected abstract int compare(Writable w1, Writable w2);
}
