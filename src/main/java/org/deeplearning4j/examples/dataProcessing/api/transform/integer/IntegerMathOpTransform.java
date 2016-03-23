package org.deeplearning4j.examples.dataProcessing.api.transform.integer;

import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.MathOp;
import org.deeplearning4j.examples.dataProcessing.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.IntegerMetaData;
import org.deeplearning4j.examples.dataProcessing.api.transform.BaseColumnTransform;

/**
 * Integer mathematical operation.
 *
 * @author Alex Black
 */
public class IntegerMathOpTransform extends BaseColumnTransform {

    private final MathOp mathOp;
    private final int scalar;

    public IntegerMathOpTransform(String columnName, MathOp mathOp, int scalar) {
        super(columnName);
        this.mathOp = mathOp;
        this.scalar = scalar;
    }

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnType) {
        if (!(oldColumnType instanceof IntegerMetaData))
            throw new IllegalStateException("Column is not an integer column");
        IntegerMetaData meta = (IntegerMetaData) oldColumnType;
        Integer minValue = meta.getMinAllowedValue();
        Integer maxValue = meta.getMaxAllowedValue();
        if (minValue != null) minValue = doOp(minValue);
        if (maxValue != null) maxValue = doOp(maxValue);
        if(minValue != null && maxValue != null && minValue > maxValue ){
            //Consider rsub 1, with original min/max of 0 and 1: (1-0) -> 1 and (1-1) -> 0
            //Or multiplication by -1: (0 to 1) -> (-1 to 0)
            //Need to swap min/max here...
            Integer temp = minValue;
            minValue = maxValue;
            maxValue = temp;
        }
        return new IntegerMetaData(minValue, maxValue);
    }

    private int doOp(int input) {
        switch (mathOp) {
            case Add:
                return input + scalar;
            case Subtract:
                return input - scalar;
            case Multiply:
                return input * scalar;
            case Divide:
                return input / scalar;
            case Modulus:
                return input % scalar;
            case ReverseSubtract:
                return scalar - input;
            case ReverseDivide:
                return scalar / input;
            case ScalarMin:
                return Math.min(input, scalar);
            case ScalarMax:
                return Math.max(input, scalar);
            default:
                throw new IllegalStateException("Unknown or not implemented math op: " + mathOp);
        }
    }

    @Override
    public Writable map(Writable columnWritable) {
        return new IntWritable(doOp(columnWritable.toInt()));
    }

    @Override
    public String toString() {
        return "IntegerMathOpTransform(mathOp=" + mathOp + ",scalar=" + scalar + ")";
    }
}
