package org.deeplearning4j.examples.dataProcessing.api;

import lombok.Data;
import org.deeplearning4j.examples.dataProcessing.api.filter.Filter;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertToSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceSplit;

import java.io.Serializable;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public class DataAction implements Serializable {

    private final Transform transform;
    private final Filter filter;
    private final ConvertToSequence convertToSequence;
    private final ConvertFromSequence convertFromSequence;
    private final SequenceSplit sequenceSplit;

    private DataAction(Transform transform, Filter filter, ConvertToSequence convertToSequence,
                       ConvertFromSequence convertFromSequence, SequenceSplit sequenceSplit ) {
        this.transform = transform;
        this.filter = filter;
        this.convertToSequence = convertToSequence;
        this.convertFromSequence = convertFromSequence;
        this.sequenceSplit = sequenceSplit;
    }

    public DataAction(Transform transform) {
        this(transform, null, null, null, null);
    }

    public DataAction(Filter filter) {
        this(null, filter, null, null, null);
    }

    public DataAction(ConvertToSequence convertToSequence) {
        this(null, null, convertToSequence, null, null);
    }

    public DataAction(ConvertFromSequence convertFromSequence) {
        this(null, null, null, convertFromSequence, null);
    }

    public DataAction(SequenceSplit sequenceSplit){
        this(null,null,null,null,sequenceSplit);
    }

}