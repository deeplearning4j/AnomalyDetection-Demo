package org.deeplearning4j.examples.data.transform.integer;

import lombok.Data;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.transform.string.BaseStringTransform;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public class ReplaceEmptyIntegerWithValueTransform extends BaseIntegerTransform {

    private final int newValueOfEmptyIntegers;

    public ReplaceEmptyIntegerWithValueTransform(String columnName, int newValueOfEmptyIntegers) {
        super(columnName);
        this.newValueOfEmptyIntegers = newValueOfEmptyIntegers;
    }

    @Override
    public Writable map(Writable writable) {
        String s = writable.toString();
        if(s == null || s.isEmpty()) return new IntWritable(newValueOfEmptyIntegers);
        return writable;
    }
}
