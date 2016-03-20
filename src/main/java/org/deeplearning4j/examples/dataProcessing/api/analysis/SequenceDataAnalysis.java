package org.deeplearning4j.examples.dataProcessing.api.analysis;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.deeplearning4j.examples.dataProcessing.api.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.analysis.sequence.SequenceLengthAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.List;

/**
 * Created by Alex on 12/03/2016.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SequenceDataAnalysis extends DataAnalysis {

    private final SequenceLengthAnalysis sequenceLengthAnalysis;

    public SequenceDataAnalysis(Schema schema, List<ColumnAnalysis> columnAnalysis, SequenceLengthAnalysis sequenceAnalysis) {
        super(schema, columnAnalysis);
        this.sequenceLengthAnalysis = sequenceAnalysis;
    }

    @Override
    public String toString(){
        return sequenceLengthAnalysis + "\n" + super.toString();
    }
}
