package org.deeplearning4j.examples.dataProcessing.api.transform;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.Transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**BaseTransform: an abstact transform class, that handles transforming sequences by transforming each example individally
 *
 * @author Alex Black
 */
public abstract class BaseTransform implements Transform {

    protected Schema inputSchema;

    @Override
    public void setInputSchema(Schema inputSchema) {
        this.inputSchema = inputSchema;
    }

    @Override
    public List<List<Writable>> mapSequence(List<List<Writable>> sequence){

        List<List<Writable>> out = new ArrayList<>(sequence.size());
        for(List<Writable> c : sequence){
            out.add(map(c));
        }
        return out;
    }
}
