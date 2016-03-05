package org.deeplearning4j.examples.data.dataquality.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor @Data
public class StringQuality implements ColumnQuality {

    private final long countEmptyString;
    private final long countAlphabetic; //A-Z, a-z only
    private final long countNumerical;  //0-9 only
    private final long countWordCharacter;   //A-Z, a-z, 0-9
    private final long countWhitespace;  //tab, spaces etc
    private final long countUnique;
    private final long countTotal;

    public StringQuality add(StringQuality other){
        return new StringQuality(countEmptyString + other.countEmptyString,
                countAlphabetic + other.countAlphabetic,
                countNumerical + other.countNumerical,
                countWordCharacter + other.countWordCharacter,
                countWhitespace + other.countWhitespace,
                countUnique + other.countUnique,
                countTotal + other.countTotal);
    }

}
