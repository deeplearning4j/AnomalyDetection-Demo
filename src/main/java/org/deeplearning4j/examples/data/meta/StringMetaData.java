package org.deeplearning4j.examples.data.meta;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.ColumnType;

/**
 * Created by Alex on 5/03/2016.
 */
public class StringMetaData implements ColumnMetaData {

    private String regex;
    private int minLength;
    private int maxLength;

    /** Default constructor with no restrictions on allowable strings */
    public StringMetaData(){
        this(null,0,Integer.MAX_VALUE);
    }

    public StringMetaData(String mustMatchRegex, int minLength, int maxLength){
        this.regex = mustMatchRegex;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }


    @Override
    public ColumnType getColumnType() {
        return ColumnType.String;
    }

    @Override
    public boolean isValid(Writable writable) {
        String str = writable.toString();
        int len = str.length();
        boolean matches = (len >= minLength && len <= maxLength);
        if(!matches) return false;
        return regex == null || str.matches(regex);
    }
}
