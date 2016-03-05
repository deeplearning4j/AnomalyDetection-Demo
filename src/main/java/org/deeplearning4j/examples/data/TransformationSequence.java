package org.deeplearning4j.examples.data;

import org.deeplearning4j.examples.data.transform.column.RemoveColumnsTransform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class TransformationSequence implements Serializable {

    private Schema initialSchema;
    private List<Transform> transformationList;

    private TransformationSequence(Builder builder){
        transformationList = builder.transformationList;
        initialSchema = builder.initialSchema;

        //Calculate and set the schemas for each tranformation:
        Schema currInputSchema = initialSchema;
        for(Transform t : transformationList){
            t.setInputSchema(currInputSchema);
            currInputSchema = t.transform(currInputSchema);
        }
    }

    public List<Transform> getTransformationList(){
        return transformationList;
    }

    public Schema getFinalSchema(Schema input){
        for(Transform t : transformationList){
            input = t.transform(input);
        }
        return input;
    }

    public static class Builder {

        private List<Transform> transformationList = new ArrayList<>();
        private Schema initialSchema;

        public Builder(Schema initialSchema){
            this.initialSchema = initialSchema;
        }

        public Builder add(Transform transformation){
            transformationList.add(transformation);
            return this;
        }

        public Builder filter(Filter filter){
            //Need to handle order...
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public Builder removeColumns(String... columnNames){
            transformationList.add(new RemoveColumnsTransform(columnNames));
            return this;
        }

        public TransformationSequence build(){
            return new TransformationSequence(this);
        }
    }

}
