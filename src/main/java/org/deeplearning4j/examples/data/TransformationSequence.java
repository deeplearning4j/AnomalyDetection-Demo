package org.deeplearning4j.examples.data;

import org.deeplearning4j.examples.data.transform.RemoveColumnsTransform;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
public class TransformationSequence {

    private List<Transform> transformationList;

    private TransformationSequence(Builder builder){
        transformationList = builder.transformationList;
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
