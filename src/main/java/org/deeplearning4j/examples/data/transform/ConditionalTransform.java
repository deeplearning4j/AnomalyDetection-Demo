package org.deeplearning4j.examples.data.transform;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.Transform;
import org.deeplearning4j.examples.data.analysis.columns.StringAnalysis;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ConditionalTransform implements Transform {


    protected final String column;
    protected Schema inputSchema;

    protected int newVal1;
    protected int newVal2;
    protected int filterCol;
    protected List<String> filterVal;


    public ConditionalTransform(String column, int newVal1, int newVal2, int filterCol, List<String> filterVal) {
        this.column = column;
        this.newVal1 = newVal1;
        this.newVal2 = newVal2;
        this.filterCol = filterCol;
        this.filterVal = filterVal;
    }


    @Override
    public void setInputSchema(Schema inputSchema){
        this.inputSchema = inputSchema;
    }

    @Override
    public Schema transform(Schema schema) {
        List<ColumnMetaData> oldMeta = schema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(oldMeta.size());

        Iterator<ColumnMetaData> typesIter = oldMeta.iterator();

        while(typesIter.hasNext()){
            ColumnMetaData t = typesIter.next();
            newMeta.add(t);
        }

        return new Schema(new ArrayList<>(schema.getColumnNames()),newMeta);
    }

    @Override
    public Collection<Writable> map(Collection<Writable> writables) {
        int n = writables.size();
        List<Writable> out = new ArrayList<>(n);

        for(Writable w : writables){
            Double val = w.toDouble();
            if(Double.isNaN(val) || val > 1) {
                if (filterVal.contains(out.get(filterCol).toString()))
                    out.add(new IntWritable(newVal1));
                else
                    out.add(new IntWritable(newVal2));
            }
        }
        return out;
    }


}
