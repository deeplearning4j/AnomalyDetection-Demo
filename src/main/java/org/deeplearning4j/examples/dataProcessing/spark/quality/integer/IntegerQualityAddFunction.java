package org.deeplearning4j.examples.dataProcessing.spark.quality.integer;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.IntegerQuality;
import org.deeplearning4j.examples.dataProcessing.api.metadata.IntegerMetaData;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class IntegerQualityAddFunction implements Function2<IntegerQuality,Writable,IntegerQuality> {

    private final IntegerMetaData meta;

    @Override
    public IntegerQuality call(IntegerQuality v1, Writable writable) throws Exception {

        long valid = v1.getCountValid();
        long invalid = v1.getCountInvalid();
        long countMissing = v1.getCountMissing();
        long countTotal = v1.getCountTotal() + 1;
        long nonInteger = v1.getCountNonInteger();

        if(meta.isValid(writable)) valid++;
        else invalid++;

        String str = writable.toString();
        try{
            Integer.parseInt(str);
        }catch(NumberFormatException e){
            nonInteger++;
        }

        return new IntegerQuality(valid,invalid,countMissing,countTotal,nonInteger);
    }
}
