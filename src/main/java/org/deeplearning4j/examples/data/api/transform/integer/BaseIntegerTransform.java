package org.deeplearning4j.examples.data.api.transform.integer;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.data.api.transform.BaseColumnTransform;

/**
 * Created by Alex on 5/03/2016.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public abstract class BaseIntegerTransform extends BaseColumnTransform {   //implements Transform {

    public BaseIntegerTransform(String column){
        super(column);
    }

    public abstract Writable map(Writable writable);

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnMeta){
        return oldColumnMeta;
    }
}
