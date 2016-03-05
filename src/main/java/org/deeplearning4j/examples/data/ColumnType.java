package org.deeplearning4j.examples.data;

/**
 * Created by Alex on 4/03/2016.
 */
public enum ColumnType {
    String,
    Integer,
    Double,
    Categorical,
    Time,
    BLOB    //Binary Large Object - i.e., like in SQL. Arbitrary byte[] data

}
