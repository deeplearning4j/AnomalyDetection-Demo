package org.deeplearning4j.examples.dataProcessing.api;

import lombok.Data;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.analysis.DataAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.analysis.columns.*;
import org.deeplearning4j.examples.dataProcessing.api.filter.Filter;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.schema.SequenceSchema;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertToSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceComparator;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceSplit;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.CategoricalToOneHotTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.DuplicateColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.RemoveColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.RenameColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.integer.IntegerMathOpTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.longtransform.LongMathOpTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.normalize.Normalize;
import org.deeplearning4j.examples.dataProcessing.api.transform.real.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A TransformProcess defines an ordered list of transformations to be executed on some data
 *
 * @author Alex Black
 */
@Data
public class TransformProcess implements Serializable {

    private final Schema initialSchema;
    private List<DataAction> actionList;

    private TransformProcess(Builder builder) {
        actionList = builder.actionList;
        initialSchema = builder.initialSchema;

        //Calculate and set the schemas for each tranformation:
        Schema currInputSchema = builder.initialSchema;
        for (DataAction d : actionList) {
            if (d.getTransform() != null) {
                Transform t = d.getTransform();
                t.setInputSchema(currInputSchema);
                currInputSchema = t.transform(currInputSchema);
            } else if (d.getFilter() != null) {
                //Filter -> doesn't change schema. But we DO need to set the schema in the filter...
                d.getFilter().setSchema(currInputSchema);
            } else if (d.getConvertToSequence() != null) {
                if (currInputSchema instanceof SequenceSchema) {
                    throw new RuntimeException("Cannot convert to sequence: schema is already a sequence schema: " + currInputSchema);
                }
                ConvertToSequence cts = d.getConvertToSequence();
                cts.setInputSchema(currInputSchema);
                currInputSchema = cts.transform(currInputSchema);
            } else if (d.getConvertFromSequence() != null) {
                ConvertFromSequence cfs = d.getConvertFromSequence();
                if (!(currInputSchema instanceof SequenceSchema)) {
                    throw new RuntimeException("Cannot convert from sequence: schema is not a sequence schema: " + currInputSchema);
                }
                cfs.setInputSchema((SequenceSchema) currInputSchema);
                currInputSchema = cfs.transform((SequenceSchema) currInputSchema);
            } else if (d.getSequenceSplit() != null) {
                continue;   //no change to sequence schema
            } else if (d.getReducer() != null) {
                IReducer reducer = d.getReducer();
                reducer.setInputSchema(currInputSchema);
                currInputSchema = reducer.transform(currInputSchema);
            } else {
                throw new RuntimeException("Unknown action: " + d);
            }
        }
    }

    public List<DataAction> getActionList() {
        return actionList;
    }

    /**
     * Get the
     *
     * @return
     */
    public Schema getFinalSchema() {
        Schema currInputSchema = initialSchema;
        for (DataAction d : actionList) {
            if (d.getTransform() != null) {
                Transform t = d.getTransform();
                currInputSchema = t.transform(currInputSchema);
            } else if (d.getFilter() != null) {
                continue; //Filter -> doesn't change schema
            } else if (d.getConvertToSequence() != null) {
                if (currInputSchema instanceof SequenceSchema) {
                    throw new RuntimeException("Cannot convert to sequence: schema is already a sequence schema: " + currInputSchema);
                }
                ConvertToSequence cts = d.getConvertToSequence();
                currInputSchema = cts.transform(currInputSchema);
            } else if (d.getConvertFromSequence() != null) {
                ConvertFromSequence cfs = d.getConvertFromSequence();
                if (!(currInputSchema instanceof SequenceSchema)) {
                    throw new RuntimeException("Cannot convert from sequence: schema is not a sequence schema: " + currInputSchema);
                }
                currInputSchema = cfs.transform((SequenceSchema) currInputSchema);
            } else if (d.getSequenceSplit() != null) {
                continue;   //Sequence split -> no change to schema
            } else if (d.getReducer() != null) {
                IReducer reducer = d.getReducer();
                currInputSchema = reducer.transform(currInputSchema);
            } else {
                throw new RuntimeException("Unknown action: " + d);
            }
        }
        return currInputSchema;
    }


    /**
     * Execute the full sequence of transformations for a single example. May return null if example is filtered
     * <b>NOTE:</b> Some TransformProcess operations cannot be done on examples individually. Most notably, ConvertToSequence
     * and ConvertFromSequence operations require the full data set to be processed at once
     *
     * @param input
     * @return
     */
    public List<Writable> execute(List<Writable> input) {
        List<Writable> currValues = input;

        for (DataAction d : actionList) {
            if (d.getTransform() != null) {
                Transform t = d.getTransform();
                currValues = t.map(currValues);

            } else if (d.getFilter() != null) {
                Filter f = d.getFilter();
                if (f.removeExample(currValues)) return null;
            } else if (d.getConvertToSequence() != null) {
                throw new RuntimeException("Cannot execute examples individually: TransformProcess contains a ConvertToSequence operation");
            } else if (d.getConvertFromSequence() != null) {
                throw new RuntimeException("Unexpected operation: TransformProcess contains a ConvertFromSequence operation");
            } else if (d.getSequenceSplit() != null) {
                throw new RuntimeException("Cannot execute examples individually: TransformProcess contains a SequenceSplit operation");
            } else {
                throw new RuntimeException("Unknown action: " + d);
            }
        }

        return currValues;
    }

    /**
     * Execute the full sequence of transformations for a single time series (sequence). May return null if example is filtered
     */
    public List<List<Writable>> executeSequence(List<List<Writable>> inputSequence) {


        throw new UnsupportedOperationException("Not yet implemented");
    }


    /**
     * Builder class for constructing a TransformProcess
     */
    public static class Builder {

        private List<DataAction> actionList = new ArrayList<>();
        private Schema initialSchema;

        public Builder(Schema initialSchema) {
            this.initialSchema = initialSchema;
        }

        /**
         * Add a transformation to be executed after the previously-added operations have been executed
         *
         * @param transform Transform to execute
         */
        public Builder transform(Transform transform) {
            actionList.add(new DataAction(transform));
            return this;
        }

        /**
         * Add a filter operation to be executed after the previously-added operations have been executed
         *
         * @param filter Filter operation to execute
         */
        public Builder filter(Filter filter) {
            actionList.add(new DataAction(filter));
            return this;
        }

        /**
         * Remove all of the specified columns, by name
         *
         * @param columnNames Names of the columns to remove
         */
        public Builder removeColumns(String... columnNames) {
            return transform(new RemoveColumnsTransform(columnNames));
        }

        /**
         * Rename a single column
         *
         * @param oldName Original column name
         * @param newName New column name
         */
        public Builder renameColumn(String oldName, String newName) {
            return transform(new RenameColumnsTransform(oldName, newName));
        }

        /**
         * Rename multiple columns
         *
         * @param oldNames List of original column names
         * @param newNames List of new column names
         */
        public Builder renameColumns(List<String> oldNames, List<String> newNames) {
            return transform(new RenameColumnsTransform(oldNames, newNames));
        }

        /**
         * Duplicate a single column
         *
         * @param columnName Name of the column to duplicate
         * @param newName    Name of the new (duplicate) column
         */
        public Builder duplicateColumn(String columnName, String newName) {
            return transform(new DuplicateColumnsTransform(Collections.singletonList(columnName), Collections.singletonList(newName)));
        }


        /**
         * Duplicate a set of columns
         *
         * @param columnNames Names of the columns to duplicate
         * @param newNames    Names of the new (duplicated) columns
         */
        public Builder duplicateColumns(List<String> columnNames, List<String> newNames) {
            return transform(new DuplicateColumnsTransform(columnNames, newNames));
        }

        /**
         * Perform a mathematical operation (add, subtract, scalar max etc) on the specified integer column
         *
         * @param columnName The integer column to perform the operation on
         * @param mathOp     The mathematical operation
         * @param scalar     The scalar value to use in the mathematical operation
         */
        public Builder integerMathOp(String columnName, MathOp mathOp, int scalar) {
            return transform(new IntegerMathOpTransform(columnName, mathOp, scalar));
        }

        /**
         * Perform a mathematical operation (add, subtract, scalar max etc) on the specified long column
         *
         * @param columnName The long column to perform the operation on
         * @param mathOp     The mathematical operation
         * @param scalar     The scalar value to use in the mathematical operation
         */
        public Builder longMathOp(String columnName, MathOp mathOp, long scalar) {
            return transform(new LongMathOpTransform(columnName, mathOp, scalar));
        }

        /**
         * Perform a mathematical operation (add, subtract, scalar max etc) on the specified double column
         *
         * @param columnName The integer column to perform the operation on
         * @param mathOp     The mathematical operation
         * @param scalar     The scalar value to use in the mathematical operation
         */
        public Builder doubleMathOp(String columnName, MathOp mathOp, double scalar) {
            return transform(new DoubleMathOpTransform(columnName, mathOp, scalar));
        }

        /**
         * Convert the specified columns from a categorical representation to a one-hot representation.
         * This involves the creation of multiple new columns each.
         *
         * @param columnNames Names of the categorical columns to convert to a one-hot representation
         */
        public Builder categoricalToOneHot(String... columnNames) {
            for (String s : columnNames) {
                transform(new CategoricalToOneHotTransform(s));
            }
            return this;
        }

        /**
         * Normalize the specified column with a given type of normalization
         *
         * @param column Column to normalize
         * @param type   Type of normalization to apply
         * @param da     DataAnalysis object
         */
        public Builder normalize(String column, Normalize type, DataAnalysis da) {

            ColumnAnalysis ca = da.getColumnAnalysis(column);
            if (!(ca instanceof NumericalColumnAnalysis))
                throw new IllegalStateException("Column \"" + column + "\" analysis is not numerical. "
                        + "Column is not numerical?");

            NumericalColumnAnalysis nca = (NumericalColumnAnalysis) ca;
            double min = nca.getMinDouble();
            double max = nca.getMaxDouble();
            double mean = nca.getMean();
            double sigma = nca.getSampleStdev();

            switch (type) {
                case MinMax:
                    return transform(new MinMaxNormalizer(column, min, max));
                case MinMax2:
                    return transform(new MinMaxNormalizer(column, min, max, -1, 1));
                case Standardize:
                    return transform(new StandardizeNormalizer(column, mean, sigma));
                case SubtractMean:
                    return transform(new SubtractMeanNormalizer(column, mean));
                case Log2Mean:
                    return transform(new Log2Normalizer(column, mean, min, 0.5));
                case Log2MeanExcludingMin:
                    long countMin = nca.getCountMinValue();

                    //mean including min value: (sum/totalCount)
                    //mean excluding min value: (sum - countMin*min)/(totalCount - countMin)
                    double meanExMin = (mean * ca.getCountTotal() - countMin * min) / (ca.getCountTotal() - countMin);
                    return transform(new Log2Normalizer(column, meanExMin, min, 0.5));
                default:
                    throw new RuntimeException("Unknown/not implemented normalization type: " + type);
            }
        }

        /**
         * Convert a set of independent records/examples into a sequence, according to some key.
         * Within each sequence, values are ordered using the provided {@link SequenceComparator}
         *
         * @param keyColumn    Column to use as a key (values with the same key will be combined into sequences)
         * @param comparator   A SequenceComparator to order the values within each sequence (for example, by time or String order)
         * @param sequenceType The type of sequence
         */
        public Builder convertToSequence(String keyColumn, SequenceComparator comparator, SequenceSchema.SequenceType sequenceType) {
            actionList.add(new DataAction(new ConvertToSequence(keyColumn, comparator, sequenceType)));
            return this;
        }

        /**
         * Split sequences into 1 or more other sequences. Used for example to split large sequences into a set of smaller sequences
         *
         * @param split SequenceSplit that defines how splits will occur
         */
        public Builder splitSequence(SequenceSplit split) {
            actionList.add(new DataAction(split));
            return this;
        }

        /**
         * Reduce (i.e., aggregate/combine) a set of examples (typically by key).
         * <b>Note</b>: In the current implementation, reduction operations can be performed only on standard (i.e., non-sequence) data
         *
         * @param reducer Reducer to use
         */
        public Builder reduce(IReducer reducer) {
            actionList.add(new DataAction(reducer));
            return this;
        }

        /**
         * Create the TransformProcess object
         */
        public TransformProcess build() {
            return new TransformProcess(this);
        }
    }


}
