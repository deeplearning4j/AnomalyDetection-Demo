package org.deeplearning4j.examples.data.spark;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.metadata.ColumnMetaData;

/**
 * Created by Alex on 6/03/2016.
 */
@AllArgsConstructor
public class FilterWritablesBySchemaFunction implements Function<Writable,Boolean> {

    private final ColumnMetaData meta;
    private final boolean keepValid;    //If true: keep valid. If false: keep invalid

    @Override
    public Boolean call(Writable v1) throws Exception {
        boolean valid = meta.isValid(v1);
        if(keepValid) return valid; //Spark: return true to keep
        else return !valid;
    }
}
