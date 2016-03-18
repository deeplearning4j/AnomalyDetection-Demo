package org.deeplearning4j.examples.dataProcessing.api.transform.string;

import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Alex on 6/03/2016.
 */
public class MapAllStringsExceptListTransform extends BaseStringTransform {

    private final Set<String> exclusionSet;
    private final String newValue;

    public MapAllStringsExceptListTransform(String column, String newValue, List<String> exceptions) {
        super(column);
        this.newValue = newValue;
        this.exclusionSet = new HashSet<>(exceptions);
    }

    @Override
    public Text map(Writable writable) {
        String str = writable.toString();
        if(exclusionSet.contains(str)){
            return new Text(str);
        } else {
            return new Text(newValue);
        }
    }
}