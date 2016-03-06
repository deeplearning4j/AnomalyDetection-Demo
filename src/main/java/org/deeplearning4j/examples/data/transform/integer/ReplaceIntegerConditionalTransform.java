package org.deeplearning4j.examples.data.transform.integer;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.analysis.columns.StringAnalysis;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;

import java.util.List;

/**
 *
 */
public class ReplaceIntegerConditionalTransform extends BaseIntegerTransform {

    protected int newVal1;
    protected int newVal2;
    protected int filterCol;
    protected List<String> filterVal;

    public ReplaceIntegerConditionalTransform(String column, int newVal1, int newVal2, int filterCol, List<String> filterVal) {
        super(column);
        this.newVal1 = newVal1;
        this.newVal2 = newVal2;
        this.filterCol = filterCol;
        this.filterVal = filterVal;
    }

    @Override
    public Writable map(Writable writable) {
        Double val = writable.toDouble();
        if(Double.isNaN(val) || val > 1) {
            if (filterVal.contains(inputSchema.getMetaData(filterCol).toString()))
                return new IntWritable(newVal1);
            else
                return new IntWritable(newVal2);
        }
        return writable;
    }
}
