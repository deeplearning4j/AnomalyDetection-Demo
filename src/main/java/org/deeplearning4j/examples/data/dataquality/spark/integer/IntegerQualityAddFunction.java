package org.deeplearning4j.examples.data.dataquality.spark.integer;

import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.dataquality.columns.IntegerQuality;

import java.util.Collection;

/**
 * Created by Alex on 5/03/2016.
 */
public class IntegerQualityAddFunction implements Function2<IntegerQuality,Writable,IntegerQuality> {
    @Override
    public IntegerQuality call(IntegerQuality v1, Writable writable) throws Exception {

        long missing = v1.getCountMissing();
        long nonInteger = v1.getCountNonInteger();
        long countTotal = v1.getCountTotal() + 1;

        String str = writable.toString();
        if(str == null || str.isEmpty()){
            missing++;
        } else {
            try{
                Integer.parseInt(str);
            }catch(NumberFormatException e){
                nonInteger++;
            }
        }

        return new IntegerQuality(missing,nonInteger,countTotal);
    }
}
