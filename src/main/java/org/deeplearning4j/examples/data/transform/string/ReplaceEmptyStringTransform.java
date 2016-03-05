package org.deeplearning4j.examples.data.transform.string;

import lombok.Data;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public class ReplaceEmptyStringTransform extends BaseStringTransform {

    private String newValueOfEmptyStrings;

    public ReplaceEmptyStringTransform(String columnName, String newValueOfEmptyStrings) {
        super(columnName);
        this.newValueOfEmptyStrings = newValueOfEmptyStrings;
    }

    @Override
    public Text map(Writable writable) {
        String s = writable.toString();
        if(s == null || s.isEmpty()) return new Text(newValueOfEmptyStrings);
        else if(writable instanceof Text) return (Text)writable;
        else return new Text(writable.toString());
    }
}
