package org.deeplearning4j.examples.dataProcessing.api.dataquality.columns;

/**
 * Created by Alex on 5/03/2016.
 */
public class TimeQuality extends ColumnQuality {


    public TimeQuality(long countValid, long countInvalid, long countMissing, long countTotal) {
        super(countValid, countInvalid, countMissing, countTotal);
    }

    @Override
    public String toString(){
        return "TimeQuality(" + super.toString() + ")";
    }
}
