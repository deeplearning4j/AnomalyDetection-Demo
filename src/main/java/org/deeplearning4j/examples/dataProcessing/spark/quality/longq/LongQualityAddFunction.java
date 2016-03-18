package org.deeplearning4j.examples.dataProcessing.spark.quality.longq;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.LongQuality;
import org.deeplearning4j.examples.dataProcessing.api.metadata.LongMetaData;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class LongQualityAddFunction implements Function2<LongQuality,Writable,LongQuality> {

    private final LongMetaData meta;

    @Override
    public LongQuality call(LongQuality v1, Writable writable) throws Exception {

        long valid = v1.getCountValid();
        long invalid = v1.getCountInvalid();
        long countMissing = v1.getCountMissing();
        long countTotal = v1.getCountTotal() + 1;
        long nonLong = v1.getCountNonLong();

        if(meta.isValid(writable)) valid++;
        else invalid++;

        String str = writable.toString();
        try{
            Long.parseLong(str);
        }catch(NumberFormatException e){
            nonLong++;
        }

        return new LongQuality(valid,invalid,countMissing,countTotal,nonLong);
    }
}
