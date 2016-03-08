package org.deeplearning4j.examples.data;

import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.IntegerAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.LongAnalysis;
import org.deeplearning4j.examples.data.analysis.columns.RealAnalysis;
import org.deeplearning4j.examples.data.transform.categorical.CategoricalToOneHotTransform;
import org.deeplearning4j.examples.data.transform.column.RemoveColumnsTransform;
import org.deeplearning4j.examples.data.transform.normalize.Normalize;
import org.deeplearning4j.examples.data.transform.real.DoubleLog2Normalizer;
import org.deeplearning4j.examples.data.transform.real.DoubleMinMaxNormalizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class TransformSequence implements Serializable {

    private Schema initialSchema;
    private List<DataAction> actionList;

    private TransformSequence(Builder builder){
        actionList = builder.actionList;
        initialSchema = builder.initialSchema;

        //Calculate and set the schemas for each tranformation:
        Schema currInputSchema = initialSchema;
//        for(Transform t : transformationList){
        for(DataAction d : actionList){
            Transform t = d.getTransform();
            if(t == null){
                //Filter -> doesn't change schema, but it does need to know the schema of the data it is filtering...
                Filter f = d.getFilter();
                f.setSchema(currInputSchema);
                continue;
            }
            t.setInputSchema(currInputSchema);
            currInputSchema = t.transform(currInputSchema);
        }
    }

    public List<DataAction> getActionList(){
        return actionList;
    }

    public Schema getFinalSchema(Schema input){
        Schema currInputSchema = input;
        for(DataAction d : actionList){
            Transform t = d.getTransform();
            if(t == null) continue; //Filter -> doesn't change schema
            t.setInputSchema(currInputSchema);
            currInputSchema = t.transform(currInputSchema);
        }
        return currInputSchema;
    }

    public static class Builder {

        private List<DataAction> actionList = new ArrayList<>();
        private Schema initialSchema;

        public Builder(Schema initialSchema){
            this.initialSchema = initialSchema;
        }

        public Builder transform(Transform transform){
            actionList.add(new DataAction(transform));
            return this;
        }

        public Builder filter(Filter filter){
            actionList.add(new DataAction(filter));
            return this;
        }

        public Builder removeColumns(String... columnNames){
            return transform(new RemoveColumnsTransform(columnNames));
        }

        public Builder categoricalToOneHot(String... columnNames){
            for(String s : columnNames){
                transform(new CategoricalToOneHotTransform(s));
            }
            return this;
        }

        public Builder normalize(String column, Normalize type, DataAnalysis da){

            switch(type){
                case MinMax:
                    return transform(new DoubleMinMaxNormalizer(column, da.getColumnAnalysis(column).getMin(), da.getColumnAnalysis(column).getMax()));
                case Log2Mean:
                    return transform(new DoubleLog2Normalizer(column, da.getColumnAnalysis(column).getMean(), da.getColumnAnalysis(column).getMin(), 0.5));
                case Log2MeanExcludingMin:
                    ColumnAnalysis ca = da.getColumnAnalysis(column);
                    double origMean = ca.getMean();
                    double min = ca.getMin();
                    long countMin;
                    //TODO: do this properly
                    if(ca instanceof IntegerAnalysis){
                        IntegerAnalysis ia = (IntegerAnalysis)ca;
                        countMin = ia.getCountMinValue();
                    } else if(ca instanceof LongAnalysis) {
                        LongAnalysis la = (LongAnalysis) ca;
                        countMin = la.getCountMinValue();
                    } else if(ca instanceof RealAnalysis){
                        RealAnalysis ra = (RealAnalysis)ca;
                        countMin = ra.getCountMinValue();
                    } else {
                        //TODO
                        throw new UnsupportedOperationException("Normalize with log2meanexcluding min not yet implemented for " + ca);
                    }

                    //mean including min value: (sum/totalCount)
                    //mean excluding min value: (sum - countMin*min)/(totalCount - countMin)
                    double meanExMin = (origMean*ca.getTotalCount() - countMin*min) / (ca.getTotalCount() - countMin);
                    return transform(new DoubleLog2Normalizer(column, meanExMin, min, 0.5));
                default:
                    throw new RuntimeException("Unknown/not implemented normalization type: " + type);
            }

        }

        public TransformSequence build(){
            return new TransformSequence(this);
        }
    }



}
