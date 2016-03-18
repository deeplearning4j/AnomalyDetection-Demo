package org.deeplearning4j.examples.data.api;

import lombok.Data;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.api.analysis.columns.ColumnAnalysis;
import org.deeplearning4j.examples.data.api.analysis.columns.IntegerAnalysis;
import org.deeplearning4j.examples.data.api.analysis.columns.LongAnalysis;
import org.deeplearning4j.examples.data.api.analysis.columns.RealAnalysis;
import org.deeplearning4j.examples.data.api.filter.Filter;
import org.deeplearning4j.examples.data.api.schema.Schema;
import org.deeplearning4j.examples.data.api.schema.SequenceSchema;
import org.deeplearning4j.examples.data.api.sequence.ConvertFromSequence;
import org.deeplearning4j.examples.data.api.sequence.ConvertToSequence;
import org.deeplearning4j.examples.data.api.sequence.SequenceComparator;
import org.deeplearning4j.examples.data.api.sequence.SequenceSplit;
import org.deeplearning4j.examples.data.api.transform.categorical.CategoricalToOneHotTransform;
import org.deeplearning4j.examples.data.api.transform.column.RemoveColumnsTransform;
import org.deeplearning4j.examples.data.api.transform.normalize.Normalize;
import org.deeplearning4j.examples.data.api.transform.real.DoubleLog2Normalizer;
import org.deeplearning4j.examples.data.api.transform.real.DoubleMinMaxNormalizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
@Data
public class TransformProcess implements Serializable {

    private final Schema initialSchema;
    private List<DataAction> actionList;

    private TransformProcess(Builder builder){
        actionList = builder.actionList;
        initialSchema = builder.initialSchema;

        //Calculate and set the schemas for each tranformation:
        Schema currInputSchema = builder.initialSchema;
//        for(Transform t : transformationList){
        for(DataAction d : actionList){
            if(d.getTransform() != null){
                Transform t = d.getTransform();
                t.setInputSchema(currInputSchema);
                currInputSchema = t.transform(currInputSchema);
            } else if(d.getFilter() != null){
                //Filter -> doesn't change schema. But we DO need to set the schema in the filter...
                d.getFilter().setSchema(currInputSchema);
            } else if(d.getConvertToSequence() != null) {
                if(currInputSchema instanceof SequenceSchema){
                    throw new RuntimeException("Cannot convert to sequence: schema is already a sequence schema: " + currInputSchema);
                }
                ConvertToSequence cts = d.getConvertToSequence();
                cts.setInputSchema(currInputSchema);
                currInputSchema = cts.transform(currInputSchema);
            } else if(d.getConvertFromSequence() != null) {
                ConvertFromSequence cfs = d.getConvertFromSequence();
                if (!(currInputSchema instanceof SequenceSchema)) {
                    throw new RuntimeException("Cannot convert from sequence: schema is not a sequence schema: " + currInputSchema);
                }
                cfs.setInputSchema((SequenceSchema) currInputSchema);
                currInputSchema = cfs.transform((SequenceSchema) currInputSchema);
            } else if(d.getSequenceSplit() != null ){
                continue;   //no change to sequence schema
            } else {
                throw new RuntimeException("Unknown action: " + d);
            }
        }
    }

    public List<DataAction> getActionList(){
        return actionList;
    }

//    public Schema getFinalSchema(Schema input){
    public Schema getFinalSchema(){
        Schema currInputSchema = initialSchema;
        for(DataAction d : actionList){
            if(d.getTransform() != null){
                Transform t = d.getTransform();
                currInputSchema = t.transform(currInputSchema);
            } else if(d.getFilter() != null){
                continue; //Filter -> doesn't change schema
            } else if(d.getConvertToSequence() != null) {
                if(currInputSchema instanceof SequenceSchema){
                    throw new RuntimeException("Cannot convert to sequence: schema is already a sequence schema: " + currInputSchema);
                }
                ConvertToSequence cts = d.getConvertToSequence();
                currInputSchema = cts.transform(currInputSchema);
            } else if(d.getConvertFromSequence() != null) {
                ConvertFromSequence cfs = d.getConvertFromSequence();
                if (!(currInputSchema instanceof SequenceSchema)) {
                    throw new RuntimeException("Cannot convert from sequence: schema is not a sequence schema: " + currInputSchema);
                }
                currInputSchema = cfs.transform((SequenceSchema) currInputSchema);
            } else if(d.getSequenceSplit() != null){
                continue;   //Sequence split -> no change to schema
            } else {
                throw new RuntimeException("Unknown action: " + d);
            }
        }
        return currInputSchema;
    }


    /** Execute the full sequence of transformations for a single example. May return null if example is filtered
     * <b>NOTE:</b> Some TransformProcess operations cannot be done on examples individually. Most notably, ConvertToSequence
     * and ConvertFromSequence operations require the full data set to be processed at once
     * @param input
     * @return
     */
    public Collection<Writable> execute(Collection<Writable> input){
        Collection<Writable> currValues = input;

        for(DataAction d : actionList){
            if(d.getTransform() != null){
                Transform t = d.getTransform();
                currValues = t.map(currValues);

            } else if(d.getFilter() != null){
                Filter f = d.getFilter();
                if(f.removeExample(currValues)) return null;
            } else if(d.getConvertToSequence() != null ) {
                throw new RuntimeException("Cannot execute examples individually: TransformProcess contains a ConvertToSequence operation");
            } else if(d.getConvertFromSequence() != null) {
                throw new RuntimeException("Unexpected operation: TransformProcess contains a ConvertFromSequence operation");
            } else if(d.getSequenceSplit() != null ){
                throw new RuntimeException("Cannot execute examples individually: TransformProcess contains a SequenceSplit operation");
            } else {
                throw new RuntimeException("Unknown action: " + d);
            }
        }

        return currValues;
    }

    /** Execute the full sequence of transformations for a single time series (sequence). May return null if example is filtered
     */
    public Collection<Collection<Writable>> executeSequence(Collection<Collection<Writable>> inputSequence ){



        throw new UnsupportedOperationException("Not yet implemented");
    }


    public static class Builder {

        private List<DataAction> actionList = new ArrayList<>();
        private Schema initialSchema;

        public Builder(Schema initialSchema){
            this.initialSchema = initialSchema;
        }

        public Builder transform(Transform transform){
            actionList.add(new DataAction(transform));
            return this;
        }

        public Builder filter(Filter filter){
            actionList.add(new DataAction(filter));
            return this;
        }

        public Builder removeColumns(String... columnNames){
            return transform(new RemoveColumnsTransform(columnNames));
        }

        public Builder categoricalToOneHot(String... columnNames){
            for(String s : columnNames){
                transform(new CategoricalToOneHotTransform(s));
            }
            return this;
        }

        public Builder normalize(String column, Normalize type, DataAnalysis da){

            switch(type){
                case MinMax:
                    return transform(new DoubleMinMaxNormalizer(column, da.getColumnAnalysis(column).getMin(), da.getColumnAnalysis(column).getMax()));
                case Log2Mean:
                    return transform(new DoubleLog2Normalizer(column, da.getColumnAnalysis(column).getMean(), da.getColumnAnalysis(column).getMin(), 0.5));
                case Log2MeanExcludingMin:
                    ColumnAnalysis ca = da.getColumnAnalysis(column);
                    double origMean = ca.getMean();
                    double min = ca.getMin();
                    long countMin;
                    //TODO: do this properly
                    if(ca instanceof IntegerAnalysis){
                        IntegerAnalysis ia = (IntegerAnalysis)ca;
                        countMin = ia.getCountMinValue();
                    } else if(ca instanceof LongAnalysis) {
                        LongAnalysis la = (LongAnalysis) ca;
                        countMin = la.getCountMinValue();
                    } else if(ca instanceof RealAnalysis){
                        RealAnalysis ra = (RealAnalysis)ca;
                        countMin = ra.getCountMinValue();
                    } else {
                        //TODO
                        throw new UnsupportedOperationException("Normalize with log2meanexcluding min not yet implemented for " + ca);
                    }

                    //mean including min value: (sum/totalCount)
                    //mean excluding min value: (sum - countMin*min)/(totalCount - countMin)
                    double meanExMin = (origMean*ca.getTotalCount() - countMin*min) / (ca.getTotalCount() - countMin);
                    return transform(new DoubleLog2Normalizer(column, meanExMin, min, 0.5));
                default:
                    throw new RuntimeException("Unknown/not implemented normalization type: " + type);
            }
        }

        public Builder convertToSequence(String keyColumn, SequenceComparator comparator, SequenceSchema.SequenceType sequenceType){
            actionList.add(new DataAction(new ConvertToSequence(keyColumn,comparator,sequenceType)));
            return this;
        }

        public Builder splitSequence(SequenceSplit split){
            actionList.add(new DataAction(split));
            return this;
        }

        public TransformProcess build(){
            return new TransformProcess(this);
        }
    }



}
