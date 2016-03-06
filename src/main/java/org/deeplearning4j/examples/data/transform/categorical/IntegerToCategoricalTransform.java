package org.deeplearning4j.examples.data.transform.categorical;

import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.meta.CategoricalMetaData;
import org.deeplearning4j.examples.data.meta.ColumnMetaData;
import org.deeplearning4j.examples.data.transform.BaseColumnTransform;

import java.util.*;

/**
 * Created by Alex on 6/03/2016.
 */
public class IntegerToCategoricalTransform extends BaseColumnTransform {

    private final Map<Integer,String> map;

    public IntegerToCategoricalTransform(String columnName, Map<Integer,String> map ){
        super(columnName);
        this.map = map;
    }

    public IntegerToCategoricalTransform(String columnName, List<String> list){
        super(columnName);
        this.map = new LinkedHashMap<>();
        int i=0;
        for(String s : list) map.put(i++,s);
    }

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnType) {
        return new CategoricalMetaData(new ArrayList<>(map.values()));
    }

    @Override
    public Writable map(Writable columnWritable) {
        return new Text(map.get(columnWritable.toInt()));
    }
}
