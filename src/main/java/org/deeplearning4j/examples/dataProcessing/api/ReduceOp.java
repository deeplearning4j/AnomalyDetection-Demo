package org.deeplearning4j.examples.dataProcessing.api;

/**ReduceOp defines the type of column reductions that can be used when reducing
 * a set of values to a single value.
 * @author Alex Black
 */
public enum ReduceOp {

    Min,
    Max,
    Range,  //Max - Min
    Sum,
    Mean,
    Stdev,
    Count,
    CountUnique,
    TakeFirst,   //First value
    TakeLast     //Last value

}
