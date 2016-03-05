package org.deeplearning4j.examples.data.transform;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.Transform;

import java.util.Collection;

/**
 * Created by Alex on 5/03/2016.
 */
public abstract class BaseTransform implements Transform {

    protected Schema inputSchema;

    @Override
    public void setInputSchema(Schema inputSchema) {
        this.inputSchema = inputSchema;
    }
}
