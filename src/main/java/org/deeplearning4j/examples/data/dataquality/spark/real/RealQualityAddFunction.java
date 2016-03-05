package org.deeplearning4j.examples.data.dataquality.spark.real;

import org.apache.spark.api.java.function.Function2;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.dataquality.columns.RealQuality;
import org.deeplearning4j.examples.data.dataquality.columns.RealQuality;

/**
 * Created by Alex on 5/03/2016.
 */
public class RealQualityAddFunction implements Function2<RealQuality,Writable,RealQuality> {
    @Override
    public RealQuality call(RealQuality v1, Writable writable) throws Exception {

        long missing = v1.getCountMissing();
        long nonReal = v1.getCountNonReal();
        long nan = v1.getCountNaN();
        long infinite = v1.getCountInfinite();
        long total = v1.getCountTotal() + 1;

        String str = writable.toString();
        if(str == null || str.isEmpty()){
            missing++;
        } else {
            double d;
            try{
                d = Double.parseDouble(str);
                if(Double.isNaN(d)) nan++;
                if(Double.isInfinite(d)) infinite++;
            }catch(NumberFormatException e){
                nonReal++;
            }
        }

        return new RealQuality(missing,nonReal,nan,infinite,total);
    }
}
