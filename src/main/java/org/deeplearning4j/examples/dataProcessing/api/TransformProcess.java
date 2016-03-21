package org.deeplearning4j.examples.dataProcessing.api;

import lombok.Data;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.analysis.DataAnalysis;
import org.deeplearning4j.examples.dataProcessing.api.analysis.columns.*;
import org.deeplearning4j.examples.dataProcessing.api.filter.Filter;
import org.deeplearning4j.examples.dataProcessing.api.reduce.IReducer;
import org.deeplearning4j.examples.dataProcessing.api.reduce.Reducer;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.schema.SequenceSchema;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.ConvertToSequence;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceComparator;
import org.deeplearning4j.examples.dataProcessing.api.sequence.SequenceSplit;
import org.deeplearning4j.examples.dataProcessing.api.transform.categorical.CategoricalToOneHotTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.column.RemoveColumnsTransform;
import org.deeplearning4j.examples.dataProcessing.api.transform.normalize.Normalize;
import org.deeplearning4j.examples.dataProcessing.api.transform.real.DoubleLog2Normalizer;
import org.deeplearning4j.examples.dataProcessing.api.transform.real.DoubleMinMaxNormalizer;
import org.deeplearning4j.examples.dataProcessing.api.transform.real.StandardizeNormalizer;
import org.deeplearning4j.examples.dataProcessing.api.transform.real.SubtractMeanNormalizer;

import java.io.Serializable;
import java.util.ArrayList;
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
                    return transform(new DoubleMinMaxNormalizer(column, min, max));
                case MinMax2:
                    return transform(new DoubleMinMaxNormalizer(column,min,max,-1,1));
                case Standardize:
                    return transform(new StandardizeNormalizer(column,mean,sigma));
                case SubtractMean:
                    return transform(new SubtractMeanNormalizer(column,mean));
                case Log2Mean:
                    return transform(new DoubleLog2Normalizer(column, mean, min, 0.5));
                case Log2MeanExcludingMin:
                    long countMin = nca.getCountMinValue();

                    //mean including min value: (sum/totalCount)
                    //mean excluding min value: (sum - countMin*min)/(totalCount - countMin)
                    double meanExMin = (mean * ca.getCountTotal() - countMin * min) / (ca.getCountTotal() - countMin);
                    return transform(new DoubleLog2Normalizer(column, meanExMin, min, 0.5));
                default:
                    throw new RuntimeException("Unknown/not implemented normalization type: " + type);
            }
        }

        /**
         * Convert a set of independent records/examples into a sequence, according to some key.
         * Within each sequence, values are ordered using the provided {@link SequenceComparator}
         *
         * @param keyColumn     Column to use as a key (values with the same key will be combined into sequences)
         * @param comparator    A SequenceComparator to order the values within each sequence (for example, by time or String order)
         * @param sequenceType  The type of sequence
         */
        public Builder convertToSequence(String keyColumn, SequenceComparator comparator, SequenceSchema.SequenceType sequenceType) {
            actionList.add(new DataAction(new ConvertToSequence(keyColumn, comparator, sequenceType)));
            return this;
        }

        /**
         * Split sequences into 1 or more other sequences. Used for example to split large sequences into a set of smaller sequences
         *
         * @param split    SequenceSplit that defines how splits will occur
         */
        public Builder splitSequence(SequenceSplit split) {
            actionList.add(new DataAction(split));
            return this;
        }

        /**
         * Reduce (i.e., aggregate/combine) a set of examples (typically by key).
         * <b>Note</b>: In the current implementation, reduction operations can be performed only on standard (i.e., non-sequence) data
         * @param reducer Reducer to use
         */
        public Builder reduce(IReducer reducer){
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
