package org.deeplearning4j.examples.data.transform;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.deeplearning4j.examples.data.ColumnType;
import org.deeplearning4j.examples.data.Schema;
import org.deeplearning4j.examples.data.Transform;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class ToOneHotTransform implements Transform {

    private int columnIdx;
    private List<String> stateNames;

    @Override
    public Schema transform(Schema schema) {

        List<String> origNames = schema.getColumnNames();
        List<ColumnType> origTypes = schema.getColumnTypes();

        int i=0;
        Iterator<String> namesIter = origNames.iterator();
        Iterator<ColumnType> typesIter = origTypes.iterator();

        List<String> outNames = new ArrayList<>(origNames.size()+stateNames.size()-1);
        List<ColumnType> outTypes = new ArrayList<>(outNames.size());

        while(namesIter.hasNext()){
            String s = namesIter.next();
            ColumnType t = typesIter.next();

            if(i++ == columnIdx){
                //Convert this to one-hot:
                int nClasses = stateNames.size();
                for (String stateName : stateNames) {
                    String newName = s + "[" + stateName + "]";
                    outNames.add(newName);
                    outTypes.add(ColumnType.Categorical);
                }
            } else {
                outNames.add(s);
                outTypes.add(t);
            }
        }

        return new Schema(outNames,outTypes);
    }
}
