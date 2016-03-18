package org.deeplearning4j.examples.data.api.transform.string;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;

import java.util.Map;

/**
 * Created by Alex on 5/03/2016.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class StringMapTransform extends BaseStringTransform {

    private final Map<String,String> map;

    /**
     *
     * @param columnName
     * @param map Key: From. Value: To
     */
    public StringMapTransform(String columnName, Map<String, String> map) {
        super(columnName);
        this.map = map;
    }

    @Override
    public Text map(Writable writable) {
        String orig = writable.toString();
        if(map.containsKey(orig)){
            return new Text(map.get(orig));
        }

        if(writable instanceof Text) return (Text)writable;
        else return new Text(writable.toString());
    }
}
