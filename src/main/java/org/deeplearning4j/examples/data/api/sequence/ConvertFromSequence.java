package org.deeplearning4j.examples.data.api.sequence;


import lombok.Data;
import org.deeplearning4j.examples.data.api.schema.Schema;
import org.deeplearning4j.examples.data.api.schema.SequenceSchema;

/**
 * Convert a set of values to a sequence
 * Created by Alex on 11/03/2016.
 */
@Data
public class ConvertFromSequence {

    private SequenceSchema inputSchema;

    public ConvertFromSequence(){

    }

    public Schema transform(SequenceSchema schema){


        throw new RuntimeException("Not yet implemented");
    }

}
