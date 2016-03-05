package org.deeplearning4j.examples.data.dataquality.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public class CategoricalQuality extends ColumnQuality {

    public CategoricalQuality(){
        super(0,0,0,0);
    }

    public CategoricalQuality(long countValid, long countInvalid, long countMissing, long countTotal){
        super(countValid,countInvalid,countMissing,countTotal);
    }

    public CategoricalQuality add(CategoricalQuality other){
        return new CategoricalQuality(
                countValid + other.countValid,
                countInvalid + other.countInvalid,
                countMissing + other.countMissing,
                countTotal + other.countTotal);
    }

    @Override
    public String toString(){
        return "CategoricalQuality(" + super.toString() + ")";
    }

}
