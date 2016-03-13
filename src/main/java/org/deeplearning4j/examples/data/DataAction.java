package org.deeplearning4j.examples.data;

import lombok.Data;
import org.deeplearning4j.examples.data.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.data.sequence.ConvertToSequence;

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

    private DataAction(Transform transform, Filter filter, ConvertToSequence convertToSequence, ConvertFromSequence convertFromSequence) {
        this.transform = transform;
        this.filter = filter;
        this.convertToSequence = convertToSequence;
        this.convertFromSequence = convertFromSequence;
    }

    public DataAction(Transform transform) {
        this(transform, null, null, null);
    }

    public DataAction(Filter filter) {
        this(null, filter, null, null);
    }

    public DataAction(ConvertToSequence convertToSequence) {
        this(null, null, convertToSequence, null);
    }

    public DataAction(ConvertFromSequence convertFromSequence) {
        this(null, null, null, convertFromSequence);
    }

}
