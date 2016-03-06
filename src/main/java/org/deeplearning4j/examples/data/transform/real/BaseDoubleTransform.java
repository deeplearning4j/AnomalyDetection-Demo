package org.deeplearning4j.examples.data.transform.real;

import lombok.Data;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.transform.BaseColumnTransform;

/**
 *
 */

@Data
public abstract class BaseDoubleTransform extends BaseColumnTransform {   //implements Transform {

    public BaseDoubleTransform(String column){
        super(column);
    }

    public abstract Writable map(Writable writable);

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnMeta){
        return oldColumnMeta;
    }
}
