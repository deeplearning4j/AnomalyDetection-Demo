package org.deeplearning4j.examples.data.sequence;


import lombok.Data;
import org.deeplearning4j.examples.data.schema.Schema;
import org.deeplearning4j.examples.data.schema.SequenceSchema;

/**
 * Convert a set of values to a sequence
 * Created by Alex on 11/03/2016.
 */
@Data
public class ConvertToSequence {

    private final String keyColumn;
    private final SequenceComparator comparator;    //For sorting values within collected (unsorted) sequence
    private final SequenceSchema.SequenceType sequenceType;
    private Schema inputSchema;

    public ConvertToSequence(String keyColumn, SequenceComparator comparator, SequenceSchema.SequenceType sequenceType){
        this.keyColumn = keyColumn;
        this.comparator = comparator;
        this.sequenceType = sequenceType;
    }

    public SequenceSchema transform(Schema schema){
        return new SequenceSchema(schema.getColumnNames(),schema.getColumnMetaData(),sequenceType);
    }

    public void setInputSchema(Schema schema){
        this.inputSchema = schema;
        comparator.setSchema(transform(schema));
    }

}
