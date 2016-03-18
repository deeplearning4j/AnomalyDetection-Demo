package org.deeplearning4j.examples.data.api.transform.categorical;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.metadata.CategoricalMetaData;
import org.deeplearning4j.examples.data.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.data.api.transform.BaseColumnTransform;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Alex on 6/03/2016.
 */
public class StringToCategoricalTransform extends BaseColumnTransform {

    private final List<String> stateNames;

    public StringToCategoricalTransform(String columnName, List<String> stateNames){
        super(columnName);
        this.stateNames = stateNames;
    }

    public StringToCategoricalTransform(String columnName, String... stateNames){
        this(columnName, Arrays.asList(stateNames));
    }

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnType) {
        return new CategoricalMetaData(stateNames);
    }

    @Override
    public Writable map(Writable columnWritable) {
        return columnWritable;
    }
}
