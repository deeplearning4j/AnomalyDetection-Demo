package org.deeplearning4j.examples.dataProcessing.api;

import lombok.Data;
import org.deeplearning4j.examples.dataProcessing.api.filter.Filter;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import org.deeplearning4j.examples.dataProcessing.api.reduce.Reducer;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertToSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceSplit;

import java.io.Serializable;

/** A helper class used in TransformProcess to store the types of action to execute next. */
@Data
public class DataAction implements Serializable {

    private Transform transform;
    private Filter filter;
    private ConvertToSequence convertToSequence;
    private ConvertFromSequence convertFromSequence;
    private SequenceSplit sequenceSplit;
    private IReducer reducer;

    public DataAction(Transform transform) {
        this.transform = transform;
    }

    public DataAction(Filter filter) {
        this.filter = filter;
    }

    public DataAction(ConvertToSequence convertToSequence) {
        this.convertToSequence = convertToSequence;
    }

    public DataAction(ConvertFromSequence convertFromSequence) {
        this.convertFromSequence = convertFromSequence;
    }

    public DataAction(SequenceSplit sequenceSplit){
        this.sequenceSplit = sequenceSplit;
    }

    public DataAction(IReducer reducer){
        this.reducer = reducer;
    }

}
