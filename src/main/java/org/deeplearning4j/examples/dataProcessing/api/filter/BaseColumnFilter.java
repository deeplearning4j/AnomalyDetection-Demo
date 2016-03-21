package org.deeplearning4j.examples.dataProcessing.api.filter;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.Collection;
import java.util.List;

/**Abstract class for filtering examples based on the values in a single column
 */
public abstract class BaseColumnFilter implements Filter {

    protected Schema schema;
    protected final String column;
    protected int columnIdx;

    protected BaseColumnFilter(String column){
        this.column = column;
    }

    @Override
    public boolean removeExample(List<Writable> writables) {
        return removeExample(writables.get(columnIdx));
    }

    @Override
    public boolean removeSequence(List<List<Writable>> sequence) {
        for(List<Writable> c : sequence){
            if(removeExample(c)) return true;
        }
        return false;
    }

    @Override
    public void setSchema(Schema schema) {
        this.schema = schema;
        this.columnIdx = schema.getIndexOfColumn(column);
    }

    /** Should the example or sequence be removed, based on the values from the specified column? */
    public abstract boolean removeExample(Writable writable);
}
