package org.deeplearning4j.examples.dataProcessing.api.condition.column;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.condition.ConditionOp;
import org.deeplearning4j.examples.dataProcessing.api.condition.SequenceConditionMode;

import java.util.Set;

/**
 * Condition that applies to the values in an Integer column, using a {@link ConditionOp}
 *
 * @author Alex Black
 */
public class IntegerColumnCondition extends BaseColumnCondition {

    private final ConditionOp op;
    private final int value;
    private final Set<Integer> set;

    /**
     * Constructor for operations such as less than, equal to, greater than, etc.
     * Uses default sequence condition mode, {@link BaseColumnCondition#DEFAULT_SEQUENCE_CONDITION_MODE}
     *
     * @param column Column to check for the condition
     * @param op     Operation (<, >=, !=, etc)
     * @param value  Value to use in the condition
     */
    public IntegerColumnCondition(String column, ConditionOp op, int value) {
        this(column, BaseColumnCondition.DEFAULT_SEQUENCE_CONDITION_MODE, op, value);
    }

    /**
     * Constructor for operations such as less than, equal to, greater than, etc.
     *
     * @param column                Column to check for the condition
     * @param sequenceConditionMode Mode for handling sequence data
     * @param op                    Operation (<, >=, !=, etc)
     * @param value                 Value to use in the condition
     */
    public IntegerColumnCondition(String column, SequenceConditionMode sequenceConditionMode,
                                  ConditionOp op, int value) {
        super(column, sequenceConditionMode);
        if (op == ConditionOp.InSet || op == ConditionOp.NotInSet) {
            throw new IllegalArgumentException("Invalid condition op: cannot use this constructor with InSet or NotInSet ops");
        }
        this.op = op;
        this.value = value;
        this.set = null;
    }

    /**
     * Constructor for operations: ConditionOp.InSet, ConditionOp.NotInSet
     * Uses default sequence condition mode, {@link BaseColumnCondition#DEFAULT_SEQUENCE_CONDITION_MODE}
     *
     * @param column Column to check for the condition
     * @param op     Operation. Must be either ConditionOp.InSet, ConditionOp.NotInSet
     * @param set    Set to use in the condition
     */
    public IntegerColumnCondition(String column, ConditionOp op, Set<Integer> set) {
        this(column, BaseColumnCondition.DEFAULT_SEQUENCE_CONDITION_MODE, op, set);
    }

    /**
     * Constructor for operations: ConditionOp.InSet, ConditionOp.NotInSet
     *
     * @param column                Column to check for the condition
     * @param sequenceConditionMode Mode for handling sequence data
     * @param op                    Operation. Must be either ConditionOp.InSet, ConditionOp.NotInSet
     * @param set                   Set to use in the condition
     */
    public IntegerColumnCondition(String column, SequenceConditionMode sequenceConditionMode,
                                  ConditionOp op, Set<Integer> set) {
        super(column, sequenceConditionMode);
        if (op != ConditionOp.InSet && op != ConditionOp.NotInSet) {
            throw new IllegalArgumentException("Invalid condition op: can ONLY use this constructor with InSet or NotInSet ops");
        }
        this.op = op;
        this.value = 0;
        this.set = set;
    }


    @Override
    public boolean columnCondition(Writable writable) {
        switch (op) {
            case LessThan:
                return writable.toInt() < value;
            case LessOrEqual:
                return writable.toInt() <= value;
            case GreaterThan:
                return writable.toInt() > value;
            case GreaterOrEqual:
                return writable.toInt() >= value;
            case Equal:
                return writable.toInt() == value;
            case NotEqual:
                return writable.toInt() != value;
            case InSet:
                return set.contains(writable.toInt());
            case NotInSet:
                return !set.contains(writable.toInt());
            default:
                throw new RuntimeException("Unknown or not implemented op: " + op);
        }
    }
}
