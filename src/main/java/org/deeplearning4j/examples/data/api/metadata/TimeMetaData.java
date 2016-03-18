package org.deeplearning4j.examples.data.api.metadata;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.api.ColumnType;

/**
 * Created by Alex on 5/03/2016.
 */
public class TimeMetaData implements ColumnMetaData {

    public enum TimeFormat {EpochSeconds, EpochMilliseconds};

    private static final long MAX_VALID_TIME_SECONDS = 4070908800L; //01 Jan 2100 00:00:00 GMT
    private static final long MAX_VALID_TIME_MILLISECONDS = 4102444800000L; //01 Jan 2100 00:00:00 GMT

    private TimeFormat timeFormat;

    public TimeMetaData(TimeFormat timeFormat){
        this.timeFormat = timeFormat;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Time;
    }

    @Override
    public boolean isValid(Writable writable) {

        switch(timeFormat){
            case EpochSeconds:
                long epochSec;
                try{
                    epochSec = Long.parseLong(writable.toString());
                }catch(NumberFormatException e ){
                    return false;
                }
                return epochSec >= 0 && epochSec <= MAX_VALID_TIME_SECONDS;
            case EpochMilliseconds:
                long epochMillisec;
                try{
                    epochMillisec = Long.parseLong(writable.toString());
                }catch(NumberFormatException e ){
                    return false;
                }
                return epochMillisec >= 0 && epochMillisec <= MAX_VALID_TIME_MILLISECONDS;
            default:
                throw new IllegalStateException("Unknown/not implemented time format: " + timeFormat);
        }
    }


    @Override
    public String toString(){
        return "TimeMetaData()";
    }
}
