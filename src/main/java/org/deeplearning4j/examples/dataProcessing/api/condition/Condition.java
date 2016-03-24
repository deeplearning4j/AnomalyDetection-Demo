package org.deeplearning4j.examples.dataProcessing.api.condition;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.List;

/**
 * The Condition interface defines a binary state that either holds/is satisfied for an example/sequence,
 * or does not hold.<br>
 * Example: number greater than x, String is one of {X,Y,Z}, etc.<br>
 * Typical uses for conditions: filtering, conditional replacement, etc
 *
 * @author Alex Black
 */
public interface Condition {

    /**
     * Is the condition satisfied for the current input/example?<br>
     * Returns true if condition is satisfied, or false otherwise.
     *
     * @param list Current example
     * @return true if condition satisfied, false otherwise
     */
    boolean condition(List<Writable> list);

    /**
     * Is the condition satisfied for the current input/sequence?<br>
     * Returns true if condition is satisfied, or false otherwise.
     *
     * @param sequence Current sequence
     * @return true if condition satisfied, false otherwise
     */
    boolean conditionSequence(List<List<Writable>> sequence);


    void setInputSchema(Schema schema);


}
