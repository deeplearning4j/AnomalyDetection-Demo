package org.deeplearning4j.examples.dataProcessing.api.reduce;

import lombok.Data;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.LongWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.ReduceOp;
import org.deeplearning4j.examples.dataProcessing.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.DoubleMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.IntegerMetaData;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.io.Serializable;
import java.util.*;

/**A Reducer is used to take a set of examples and reduce them.
 * The idea: suppose you have a large number of columns, and you want to combine/reduce the values in each column.<br>
 * Reducer allows you to specify different reductions for differently for different columns: min, max, sum, mean etc.
 * See {@link Builder} and {@link ReduceOp} for the full list.<br>
 *
 * Uses are:
 * (1) Reducing examples by a key
 * (2) Reduction operations in time series (windowing ops, etc)
 *
 * @author Alex Black
 */
@Data
public class Reducer implements IReducer, Serializable {

    private Schema schema;
    private final Set<String> keyColumns;
    private final ReduceOp defaultOp;
    private final Map<String,ReduceOp> opMap;

    private Reducer(Builder builder){
        if(builder.keyColumns == null || builder.keyColumns.length == 0){
            throw new IllegalArgumentException("No keys specified");
        }
        this.keyColumns = new HashSet<>();
        Collections.addAll(keyColumns,builder.keyColumns);
        this.defaultOp = builder.defaultOp;
        this.opMap = builder.opMap;
    }

    @Override
    public void setInputSchema(Schema schema){
        this.schema = schema;
    }

    /** Get the output schema, given the input schema */
    @Override
    public Schema transform(Schema schema){
        int nCols = schema.numColumns();
        List<String> colNames = schema.getColumnNames();
        List<ColumnMetaData> meta = schema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(nCols);

        for( int i=0; i<nCols; i++ ){
            String name = colNames.get(i);
            ColumnMetaData inMeta = meta.get(i);

            if(keyColumns.contains(name)){
                //No change to key columns
                newMeta.add(inMeta);
                continue;
            }

            ReduceOp op = opMap.get(name);
            if(op == null) op = defaultOp;

            switch(op){
                case Min:
                case Max:
                case Range:
                case TakeFirst:
                case TakeLast:
                    //No change
                    //TODO: what if the inMeta has restrictions that no longer necessarily hold?
                    newMeta.add(inMeta);
                    break;
                case Sum:
                case Mean:
                case Stdev:
                    //Always double
                    newMeta.add(new DoubleMetaData());
                    break;
                case Count:
                case CountUnique:
                    //Always integer
                    newMeta.add(new IntegerMetaData(0,null));
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown or not implemented op: " + op);
            }
        }

        return schema.newSchema(colNames,newMeta);
    }

    @Override
    public List<Writable> reduce(List<List<Writable>> examplesList){

        //Go through each writable, and reduce according to whatever strategy is specified

        int nCols = schema.numColumns();
        List<String> colNames = schema.getColumnNames();

        List<Writable> out = new ArrayList<>(nCols);
        List<Writable> tempColumnValues = new ArrayList<>(examplesList.size());
        for( int i=0; i<nCols; i++ ){
            String colName = colNames.get(i);
            if (keyColumns.contains(colName)) {
                //This is a key column -> all values should be identical
                //Therefore just take the first one
                out.add(examplesList.get(0).get(i));
                continue;
            }

            //What type of column is this?
            ColumnType type = schema.getType(i);

            //What op are we performing on this column?
            ReduceOp op = opMap.get(colName);
            if(op == null) op = defaultOp;

            //Extract out the Writables for the column we are considering here...
            for( List<Writable> list : examplesList ){
                tempColumnValues.add(list.get(i));
            }

            //Execute the reduction, store the result
            out.add(reduceColumn(op,type,tempColumnValues));

            tempColumnValues.clear();;
        }

        return out;
    }

    private Writable reduceColumn(ReduceOp op, ColumnType type, List<Writable> values){
        switch(type){
            case Integer:
            case Long:
                return reduceLongColumn(op,values);
            case Double:
                return reduceDoubleColumn(op,values);
            case String:
            case Categorical:
                return reduceStringOrCategoricalColumn(op,values);
            case Time:
                return reduceTimeColumn(op,values);
            case Bytes:
                return reduceBytesColumn(op,values);
            default:
                throw new UnsupportedOperationException("Unknown or not implemented column type: " + type);
        }
    }

    private Writable reduceLongColumn(ReduceOp op, List<Writable> values){
        switch(op){
            case Min:
                long min = Long.MAX_VALUE;
                for( Writable w : values ){
                    min = Long.min(min,w.toLong());
                }
                return new LongWritable(min);
            case Max:
                long max = Long.MIN_VALUE;
                for( Writable w : values ){
                    max = Long.max(max,w.toLong());
                }
                return new LongWritable(max);
            case Range:
                long min2 = Long.MAX_VALUE;
                long max2 = Long.MIN_VALUE;
                for(Writable w : values){
                    long l = w.toLong();
                    min2 = Math.min(min2,l);
                    max2 = Math.max(max2,l);
                }
                return new LongWritable(max2-min2);
            case Sum:
                long sum = 0;
                for(Writable w : values) sum += w.toLong();
                return new LongWritable(sum);
            case Mean:
                double sumd = 0.0;
                for(Writable w : values) sumd += w.toLong();
                return new DoubleWritable(sumd / values.size());
            case Stdev:
                double[] arr = new double[values.size()];
                int i=0;
                for(Writable w : values) arr[i++] = w.toLong();
                return new DoubleWritable(new StandardDeviation().evaluate(arr));
            case Count:
                return new IntWritable(values.size());
            case CountUnique:
                Set<Long> set = new HashSet<>();
                for(Writable w : values) set.add(w.toLong());
                return new IntWritable(set.size());
            case TakeFirst:
                return values.get(0);
            case TakeLast:
                return values.get(values.size()-1);
            default:
                throw new UnsupportedOperationException("Unknown or not implement op: " + op);
        }
    }

    private Writable reduceDoubleColumn(ReduceOp op, List<Writable> values){
        switch(op){
            case Min:
                double min = Double.MAX_VALUE;
                for( Writable w : values ){
                    min = Double.min(min,w.toLong());
                }
                return new DoubleWritable(min);
            case Max:
                double max = -Double.MAX_VALUE;
                for( Writable w : values ){
                    max = Double.max(max,w.toLong());
                }
                return new DoubleWritable(max);
            case Range:
                double min2 = Double.MAX_VALUE;
                double max2 = -Double.MAX_VALUE;
                for(Writable w : values){
                    double d = w.toDouble();
                    min2 = Math.min(min2,d);
                    max2 = Math.max(max2,d);
                }
                return new DoubleWritable(max2-min2);
            case Sum:
                double sum = 0;
                for(Writable w : values) sum += w.toDouble();
                return new DoubleWritable(sum);
            case Mean:
                double sumd = 0.0;
                for(Writable w : values) sumd += w.toDouble();
                return new DoubleWritable(sumd / values.size());
            case Stdev:
                double[] arr = new double[values.size()];
                int i=0;
                for(Writable w : values) arr[i++] = w.toLong();
                return new DoubleWritable(new StandardDeviation().evaluate(arr));
            case Count:
                return new IntWritable(values.size());
            case CountUnique:
                Set<Double> set = new HashSet<>();
                for(Writable w : values) set.add(w.toDouble());
                return new IntWritable(set.size());
            case TakeFirst:
                return values.get(0);
            case TakeLast:
                return values.get(values.size()-1);
            default:
                throw new UnsupportedOperationException("Unknown or not implement op: " + op);
        }
    }

    private Writable reduceStringOrCategoricalColumn(ReduceOp op, List<Writable> list){
        switch(op){
            case Count:
                return new IntWritable(list.size());
            case CountUnique:
                Set<String> set = new HashSet<>();
                for(Writable w : list) set.add(w.toString());
                return new IntWritable(set.size());
            case TakeFirst:
                return list.get(0);
            case TakeLast:
                return list.get(list.size()-1);
            default:
                throw new UnsupportedOperationException("Cannot execute op \"" + op + "\" on String/Categorical column "
                    + "(can only perform Count, CountUnique, TakeFirst and TakeLast ops on categorical columns)");
        }
    }

    private Writable reduceTimeColumn(ReduceOp op, List<Writable> values){
        throw new UnsupportedOperationException("Reduce ops for time columns: not yet implemented");
    }

    private Writable reduceBytesColumn(ReduceOp op, List<Writable> list){
        if(op == ReduceOp.TakeFirst) return list.get(0);
        throw new UnsupportedOperationException("Cannot execute op \"" + op + "\" on Bytes column "
                + "(can only perform TakeFirst ops on categorical columns)");
    }


    public static class Builder {

        private ReduceOp defaultOp;
        private Map<String,ReduceOp> opMap = new HashMap<>();
        private String[] keyColumns;


        /**Create a Reducer builder, and set the default column reduction operation.
         * For any columns that aren't specified explicitly, they will use the default reduction operation.
         * If a column does have a reduction operation explicitly specified, then it will override
         * the default specified here.
         *
         * @param defaultOp Default reduction operation to perform
         */
        public Builder(ReduceOp defaultOp){
            this.defaultOp = defaultOp;
        }

        /** Specify the key columns. The idea here is to be able to create a (potentially compound) key
         * out of multiple columns, using the toString representation of the values in these columns
         *
         * @param keyColumns Columns that will make up the key
         * @return
         */
        public Builder keyColumns(String... keyColumns){
            this.keyColumns = keyColumns;
            return this;
        }

        private Builder add(ReduceOp op, String[] cols){
            for(String s : cols){
                opMap.put(s,op);
            }
            return this;
        }

        /** Reduce the specified columns by taking the minimum value */
        public Builder minColumns(String... columns){
            return add(ReduceOp.Min,columns);
        }

        /** Reduce the specified columns by taking the maximum value */
        public Builder maxColumn(String... columns){
            return add(ReduceOp.Max,columns);
        }

        /** Reduce the specified columns by taking the sum of values */
        public Builder sumColumns(String... columns){
            return add(ReduceOp.Sum,columns);
        }

        /** Reduce the specified columns by taking the mean of the values */
        public Builder meanColumns(String... columns){
            return add(ReduceOp.Mean,columns);
        }

        /** Reduce the specified columns by taking the standard deviation of the values */
        public Builder stdevColumns(String... columns){
            return add(ReduceOp.Stdev,columns);
        }

        /** Reduce the specified columns by counting the number of values */
        public Builder countColumns(String... columns){
            return add(ReduceOp.Count,columns);
        }

        /** Reduce the specified columns by taking the range (max-min) of the values */
        public Builder rangeColumns(String... columns){
            return add(ReduceOp.Range,columns);
        }

        /** Reduce the specified columns by counting the number of unique values */
        public Builder countUniqueColumns(String... columns){
            return add(ReduceOp.CountUnique,columns);
        }
    }

}
