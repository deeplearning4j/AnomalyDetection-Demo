package org.deeplearning4j.examples.data.transform.integer;

import lombok.Data;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.meta.StringMetaData;
import org.deeplearning4j.examples.data.transform.BaseColumnTransform;

/**
 * Created by Alex on 5/03/2016.
 */
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
