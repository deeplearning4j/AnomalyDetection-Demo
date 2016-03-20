package org.deeplearning4j.examples.dataProcessing.api.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

/**
 * Analysis for bytes (byte[]) columns
 *
 * @author Alex Black
 */
@AllArgsConstructor
@Data
public class BytesAnalysis implements ColumnAnalysis {

    private final long countTotal;
    private final long countNull;
    private final long countZeroLength;
    private final int minNumBytes;
    private final int maxNumBytes;

    public BytesAnalysis(Builder builder) {
        this.countTotal = builder.countTotal;
        this.countNull = builder.countNull;
        this.countZeroLength = builder.countZeroLength;
        this.minNumBytes = builder.minNumBytes;
        this.maxNumBytes = builder.maxNumBytes;
    }


    @Override
    public String toString() {
        return "BytesAnalysis()";
    }


    @Override
    public long getCountTotal() {
        return countTotal;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Bytes;
    }

    public static class Builder {
        private long countTotal;
        private long countNull;
        private long countZeroLength;
        private int minNumBytes;
        private int maxNumBytes;

        public Builder countTotal(long countTotal) {
            this.countTotal = countTotal;
            return this;
        }

        public Builder countNull(long countNull) {
            this.countNull = countNull;
            return this;
        }

        public Builder countZeroLength(long countZeroLength) {
            this.countZeroLength = countZeroLength;
            return this;
        }

        public Builder minNumBytes(int minNumBytes) {
            this.minNumBytes = minNumBytes;
            return this;
        }

        public Builder maxNumBytes(int maxNumBytes) {
            this.maxNumBytes = maxNumBytes;
            return this;
        }

        public BytesAnalysis build() {
            return new BytesAnalysis(this);
        }
    }

}
