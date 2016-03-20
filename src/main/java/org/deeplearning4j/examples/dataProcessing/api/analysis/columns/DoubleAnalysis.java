package org.deeplearning4j.examples.dataProcessing.api.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

/**
 * Analysis for Double columns
 *
 * @author Alex Black
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DoubleAnalysis extends NumericalColumnAnalysis {

    private final double min;
    private final double max;

    private DoubleAnalysis(Builder builder) {
        super(builder);
        this.min = builder.min;
        this.max = builder.max;
    }

    @Override
    public String toString() {
        return "DoubleAnalysis(min=" + min + ",max=" + max + "," + super.toString() + ")";
    }

    @Override
    public double getMinDouble() {
        return min;
    }

    @Override
    public double getMaxDouble() {
        return max;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Double;
    }

    public static class Builder extends NumericalColumnAnalysis.Builder<Builder> {

        private double min;
        private double max;

        public Builder min(double min) {
            this.min = min;
            return this;
        }

        public Builder max(double max) {
            this.max = max;
            return this;
        }

        public DoubleAnalysis build() {
            return new DoubleAnalysis(this);
        }
    }
}
