package org.deeplearning4j.examples.dataProcessing.spark.quality.real;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.dataquality.columns.RealQuality;
import org.deeplearning4j.examples.dataProcessing.api.metadata.DoubleMetaData;

/**
 * Created by Alex on 5/03/2016.
 */
@AllArgsConstructor
public class RealQualityAddFunction implements Function2<RealQuality,Writable,RealQuality> {

    private final DoubleMetaData meta;

    @Override
    public RealQuality call(RealQuality v1, Writable writable) throws Exception {

        long valid = v1.getCountValid();
        long invalid = v1.getCountInvalid();
        long countMissing = v1.getCountMissing();
        long countTotal = v1.getCountTotal() + 1;
        long nonReal = v1.getCountNonReal();
        long nan = v1.getCountNaN();
        long infinite = v1.getCountInfinite();

        if(meta.isValid(writable)) valid++;
        else invalid++;

        String str = writable.toString();
        double d;
        try{
            d = Double.parseDouble(str);
            if(Double.isNaN(d)) nan++;
            if(Double.isInfinite(d)) infinite++;
        }catch(NumberFormatException e){
            nonReal++;
        }

        return new RealQuality(valid,invalid,countMissing,countTotal,nonReal,nan,infinite);
    }
}
