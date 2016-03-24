package org.deeplearning4j.examples.dataProcessing.api.condition.column;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.condition.Condition;
import org.deeplearning4j.examples.dataProcessing.api.condition.SequenceConditionMode;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.List;

/**
 * Created by Alex on 24/03/2016.
 */
public abstract class BaseColumnCondition implements Condition {

    protected final String column;
    protected int columnIdx = -1;
    protected Schema schema;
    protected SequenceConditionMode sequenceMode;

    protected BaseColumnCondition(String column, SequenceConditionMode sequenceConditionMode){
        this.column = column;
        this.sequenceMode = sequenceConditionMode;
    }

    @Override
    public void setInputSchema(Schema schema){
        columnIdx = schema.getColumnNames().indexOf(column);
        if(columnIdx < 0 ){
            throw new IllegalStateException("Invalid state: column \"" + column + "\" not present in input schema");
        }
        this.schema = schema;
    }

    @Override
    public boolean condition(List<Writable> list){
        return columnCondition(list.get(columnIdx));
    }

    @Override
    public boolean conditionSequence(List<List<Writable>> list){
        switch(sequenceMode){
            case And:
                for(List<Writable> l : list){
                    if(!condition(l)) return false;
                }
                return true;
            case Or:
                for(List<Writable> l : list){
                    if(condition(l)) return true;
                }
                return false;
            case NoSequenceMode:
                throw new IllegalStateException("Column condition " + toString() + " does not support sequence execution");
            default:
                throw new RuntimeException("Unknown/not implemented sequence mode: " + sequenceMode);
        }
    }

    public abstract boolean columnCondition(Writable writable);

}
