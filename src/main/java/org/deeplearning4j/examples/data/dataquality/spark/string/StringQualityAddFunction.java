package org.deeplearning4j.examples.data.dataquality.spark.string;

import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.dataquality.columns.StringQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class StringQualityAddFunction implements Function2<StringQuality,Writable,StringQuality> {

    //TODO: add matching of arbitrary regexes here

    @Override
    public StringQuality call(StringQuality v1, Writable writable) throws Exception {

        long empty = v1.getCountEmptyString();
        long alphabetic = v1.getCountAlphabetic();
        long numerical = v1.getCountNumerical();
        long word = v1.getCountWordCharacter();
        long whitespaceOnly = v1.getCountWhitespace();
        long countTotal = v1.getCountTotal() + 1;

        String str = writable.toString();
        if(str == null || str.isEmpty()){
            empty++;
        } else {
            if(str.matches("[a-zA-Z]")) alphabetic++;
            if(str.matches("\\d+")) numerical++;
            if(str.matches("\\w+")) word++;
            if(str.matches("\\s+")) whitespaceOnly++;
        }

        return new StringQuality(empty,alphabetic,numerical,word,whitespaceOnly,0,countTotal);
    }
}
