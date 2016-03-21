package org.deeplearning4j.examples.dataProcessing.api.transform;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class ConditionalTransform extends BaseTransform {


    protected final String column;

    protected int newVal1;
    protected int newVal2;
//    protected int filterCol;
    protected int filterColIdx;
    protected final String filterCol;
    protected List<String> filterVal;


    public ConditionalTransform(String column, int newVal1, int newVal2, String filterCol, List<String> filterVal) {
        this.column = column;
        this.newVal1 = newVal1;
        this.newVal2 = newVal2;
        this.filterCol = filterCol;
        this.filterVal = filterVal;
    }

    @Override
    public Schema transform(Schema schema) {
        return schema;
    }

    @Override
    public void setInputSchema(Schema inputSchema){
        super.setInputSchema(inputSchema);
        this.filterColIdx = inputSchema.getIndexOfColumn(filterCol);
    }

    @Override
    public List<Writable> map(List<Writable> writables) {
        List<Writable> out = new ArrayList<>(writables);

        int idx = inputSchema.getIndexOfColumn(column);
        double val = Double.NaN;
        try{
            val = out.get(idx).toDouble();
        } catch(NumberFormatException e){

        }
        if( Double.isNaN(val) || val > 1) {
            if (filterVal.contains(out.get(filterColIdx).toString()))
                out.set(idx, new IntWritable(newVal1));
            else
                out.set(idx, new IntWritable(newVal2));
        }
        return out;
    }


}
