package org.deeplearning4j.examples.data.transform.string;

import lombok.Data;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;

/**
 */
@Data
public class RemoveWhiteSpaceTransform extends BaseStringTransform {


    public RemoveWhiteSpaceTransform(String columnName) {
        super(columnName);
    }

    @Override
    public Text map(Writable writable) {
        String value = writable.toString().replaceAll("\\s","");
        return new Text(value);
    }
}
