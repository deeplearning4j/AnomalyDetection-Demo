package org.deeplearning4j.examples.dataProcessing.api.transform;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.Transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
public abstract class BaseTransform implements Transform {

    protected Schema inputSchema;

    @Override
    public void setInputSchema(Schema inputSchema) {
        this.inputSchema = inputSchema;
    }

    @Override
    public Collection<Collection<Writable>> mapSequence(Collection<Collection<Writable>> sequence){

        List<Collection<Writable>> out = new ArrayList<>(sequence.size());
        for(Collection<Writable> c : sequence){
            out.add(map(c));
        }
        return out;
    }
}
