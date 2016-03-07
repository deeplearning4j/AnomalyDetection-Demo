package org.deeplearning4j.examples.data;

import org.deeplearning4j.examples.data.analysis.DataAnalysis;
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

        //This is kinda ugly
        public Builder normalize(String column, Normalize type, DataAnalysis da){

            switch(type){
                case MinMax:
                    return transform(new DoubleMinMaxNormalizer(column, da.getColumnAnalysis(column).getMin(), da.getColumnAnalysis(column).getMax()));
                case Log2Mean0Min:
                    return transform(new DoubleLog2Normalizer(column,  da.getColumnAnalysis(column).getMean(),0.5));
                default:
                    throw new RuntimeException("Unknown/not implemented normalization type: " + type);
            }

        }

        public TransformSequence build(){
            return new TransformSequence(this);
        }
    }



}
