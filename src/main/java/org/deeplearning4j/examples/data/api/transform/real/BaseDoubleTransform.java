package org.deeplearning4j.examples.data.api.transform.real;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.data.api.metadata.DoubleMetaData;
import org.deeplearning4j.examples.data.api.transform.BaseColumnTransform;

/**
 *
 */

@EqualsAndHashCode(callSuper = true)
@Data
public abstract class BaseDoubleTransform extends BaseColumnTransform {   //implements Transform {

    public BaseDoubleTransform(String column){
        super(column);
    }

    public abstract Writable map(Writable writable);

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnMeta){
//        return oldColumnMeta;
        if(oldColumnMeta instanceof DoubleMetaData) return oldColumnMeta;
        else return new DoubleMetaData();
    }
}