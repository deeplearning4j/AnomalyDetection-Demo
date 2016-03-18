package org.deeplearning4j.examples.dataProcessing.api.metadata;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Alex on 5/03/2016.
 */
public class CategoricalMetaData implements ColumnMetaData {

    private List<String> stateNames;
    private Set<String> stateNamesSet;

    public CategoricalMetaData(String... stateNames){
        this(Arrays.asList(stateNames));
    }

    public CategoricalMetaData(List<String> stateNames){
        this.stateNames = stateNames;
        stateNamesSet = new HashSet<>(stateNames);
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Categorical;
    }

    @Override
    public boolean isValid(Writable writable) {
        return stateNamesSet.contains(writable.toString());
    }

    public List<String> getStateNames(){
        return stateNames;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("CategoricalMetaData(");
        boolean first = true;
        for(String s : stateNamesSet){
            if(!first) sb.append(",");
            sb.append(s);
            first = false;
        }
        sb.append(")");
        return sb.toString();
    }
}
