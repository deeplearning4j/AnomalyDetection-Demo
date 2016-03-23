package org.deeplearning4j.examples.dataProcessing.api.metadata;

import lombok.Data;
import org.canova.api.io.data.LongWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;

/**
 * TimeMetaData: Meta data for a date/time column.
 * <b>NOTE:</b> Time values are stored in epoch (millisecond) format.
 *
 * @author Alex Black
 */
@Data
public class TimeMetaData implements ColumnMetaData {

    private final DateTimeZone timeZone;
    private final Long minValidTime;
    private final Long maxValidTime;

    /**
     * Create a TimeMetaData column with no restriction on the allowable times
     *
     * @param timeZone Timezone for this column. Typically used for parsing
     */
    public TimeMetaData(TimeZone timeZone) {
        this(timeZone, null, null);
    }

    /**
     * @param timeZone     Timezone for this column. Typically used for parsing and some transforms
     * @param minValidTime Minimum valid time, in milliseconds (timestamp format). If null: no restriction
     * @param maxValidTime Maximum valid time, in milliseconds (timestamp format). If null: no restriction
     */
    public TimeMetaData(TimeZone timeZone, Long minValidTime, Long maxValidTime) {
        this.timeZone = DateTimeZone.forTimeZone(timeZone);
        this.minValidTime = minValidTime;
        this.maxValidTime = maxValidTime;
    }

    /**
     * @param timeZone     Timezone for this column. Typically used for parsing and some transforms
     * @param minValidTime Minimum valid time, in milliseconds (timestamp format). If null: no restriction
     * @param maxValidTime Maximum valid time, in milliseconds (timestamp format). If null: no restriction
     */
    public TimeMetaData(DateTimeZone timeZone, Long minValidTime, Long maxValidTime) {
        this.timeZone = timeZone;
        this.minValidTime = minValidTime;
        this.maxValidTime = maxValidTime;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Time;
    }

    @Override
    public boolean isValid(Writable writable) {
        long epochMillisec;

        if (writable instanceof LongWritable) {
            epochMillisec = writable.toLong();
        } else {
            try {
                epochMillisec = Long.parseLong(writable.toString());
            } catch (NumberFormatException e) {
                return false;
            }
        }
        if (minValidTime != null && epochMillisec < minValidTime) return false;
        return !(maxValidTime != null && epochMillisec > maxValidTime);
    }

    @Override
    public TimeMetaData clone() {
        return new TimeMetaData(timeZone,minValidTime,maxValidTime);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TimeMetaData(timeZone=").append(timeZone.getID());
        if (minValidTime != null) sb.append("minValidTime=").append(minValidTime);
        if (maxValidTime != null) {
            if (minValidTime != null) sb.append(",");
            sb.append("maxValidTime=").append(maxValidTime);
        }
        sb.append(")");
        return sb.toString();
    }
}
