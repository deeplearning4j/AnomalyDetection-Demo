package org.deeplearning4j.examples.data.api;

/**
 * Created by Alex on 4/03/2016.
 */
public enum ColumnType {
    String,
    Integer,
    Long,
    Double,
    Categorical,
    Time,
    BLOB    //Binary Large Object - i.e., like in SQL. Arbitrary byte[] data

}
