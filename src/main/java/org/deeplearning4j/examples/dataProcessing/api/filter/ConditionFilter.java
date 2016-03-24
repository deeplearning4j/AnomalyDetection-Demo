package org.deeplearning4j.examples.dataProcessing.api.filter;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.condition.Condition;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.List;

/**
 * A filter based on a {@link org.deeplearning4j.examples.dataProcessing.api.condition.Condition}.<br>
 * If condition is satisfied (returns true): remove the example or sequence<br>
 * If condition is not satisfied (returns false): keep the example or sequence
 *
 * @author Alex Black
 */
public class ConditionFilter implements Filter {

    private final Condition condition;

    public ConditionFilter(Condition condition){
        this.condition = condition;
    }

    @Override
    public boolean removeExample(List<Writable> writables) {
        return condition.condition(writables);
    }

    @Override
    public boolean removeSequence(List<List<Writable>> sequence) {
        return condition.conditionSequence(sequence);
    }

    @Override
    public void setSchema(Schema schema) {
        //No op
    }
}
