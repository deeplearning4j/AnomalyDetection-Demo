package org.deeplearning4j.examples.data;

/**
 * Created by Alex on 4/03/2016.
 */
public interface Transform {

    public Schema transform(Schema schema);

}
