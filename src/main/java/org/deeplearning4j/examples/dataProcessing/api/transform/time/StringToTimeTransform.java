package org.deeplearning4j.examples.dataProcessing.api.transform.time;

import org.canova.api.io.data.LongWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.TimeMetaData;
import org.deeplearning4j.examples.dataProcessing.api.transform.BaseColumnTransform;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.TimeZone;

/**
 * Convert a String column to a time column by parsing the date/time String, using a JodaTime
 *
 * @author Alex Black
 */
public class StringToTimeTransform extends BaseColumnTransform {

    private final TimeZone timeZone;
    private final Long minValidTime;
    private final Long maxValidTime;

    private final DateTimeFormatter formatter;

    public StringToTimeTransform(String columnName, String timeFormat, TimeZone timeZone){
        this(columnName,timeFormat, timeZone,null,null);

    }

    public StringToTimeTransform(String columnName, String timeFormat, TimeZone timeZone, Long minValidTime, Long maxValidTime) {
        this(columnName,DateTimeFormat.forPattern(timeFormat).withZone(DateTimeZone.forTimeZone(timeZone)),
                timeZone, minValidTime, maxValidTime);
    }

    public StringToTimeTransform(String columnName, String timeFormat, DateTimeZone timeZone, Long minValidTime, Long maxValidTime) {
        this(columnName,DateTimeFormat.forPattern(timeFormat).withZone(timeZone), timeZone.toTimeZone(), minValidTime, maxValidTime);
    }

    public StringToTimeTransform(String columnName, DateTimeFormatter formatter, TimeZone timeZone, Long minValidTime, Long maxValidTime){
        super(columnName);
        this.timeZone = timeZone;
        this.minValidTime = minValidTime;
        this.maxValidTime = maxValidTime;

        this.formatter = formatter;
    }

    @Override
    public ColumnMetaData getNewColumnMetaData(ColumnMetaData oldColumnType) {
        return new TimeMetaData(timeZone,minValidTime,maxValidTime);
    }

    @Override
    public Writable map(Writable columnWritable) {
        String str = columnWritable.toString();
        long time = formatter.parseMillis(str);
        return new LongWritable(time);
    }
}
