package org.deeplearning4j.examples.data.transform.string;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.Transform;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.meta.StringMetaData;
import org.deeplearning4j.examples.data.transform.BaseColumnTransform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public abstract class BaseStringTransform extends BaseColumnTransform {   //implements Transform {

    public BaseStringTransform(String column){
        super(column);
    }

    public abstract Text map(Writable writable);

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnType){
        return new StringMetaData();
    }
}
