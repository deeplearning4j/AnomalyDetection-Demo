package org.deeplearning4j.examples.data.meta;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.ColumnType;

import java.io.Serializable;

/**
 * Created by Alex on 5/03/2016.
 */
public interface ColumnMetaData extends Serializable {

    ColumnType getColumnType();

    boolean isValid(Writable writable);

}
