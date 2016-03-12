package org.deeplearning4j.examples.data.analysis;

import lombok.Data;
import org.deeplearning4j.examples.data.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.data.analysis.sequence.SequenceLengthAnalysis;
import org.deeplearning4j.examples.data.schema.Schema;

import java.util.List;

/**
 * Created by Alex on 12/03/2016.
 */
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
