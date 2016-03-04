package org.deeplearning4j.examples.data.spark;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.StringSplit;
import org.canova.api.writable.Writable;
import org.canova.api.writable.Writables;

import java.util.Collection;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor
public class StringToWritablesFunction implements Function<String,Collection<Writable>> {

    private RecordReader recordReader;

    @Override
    public Collection<Writable> call(String s) throws Exception {
        recordReader.initialize(new StringSplit(s));
        return recordReader.next();
    }
}
