package org.deeplearning4j.examples.dataProcessing.api.metadata;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;

/**Metadata for an integer column
 * @author Alex Black
 */
public class IntegerMetaData implements ColumnMetaData {

    //min/max are nullable: null -> no restriction on min/max values
    private final Integer min;
    private final Integer max;

    public IntegerMetaData(){
        this(null,null);
    }

    /**
     *
     * @param min Min allowed value. If null: no restriction on min value value in this column
     * @param max Max allowed value. If null: no restiction on max value in this column
     */
    public IntegerMetaData(Integer min, Integer max){
        this.min = min;
        this.max = max;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.Integer;
    }

    @Override
    public boolean isValid(Writable writable) {
        int value;
        try{
            value = Integer.parseInt(writable.toString());
        }catch(NumberFormatException e){
            return false;
        }

        if(min != null && value < min) return false;
        if(max != null && value > max) return false;
        return true;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("IntegerMetaData(");
        if(min != null) sb.append("minAllowed=").append(min);
        if(max != null){
            if(min != null) sb.append(",");
            sb.append("maxAllowed=").append(max);
        }
        sb.append(")");
        return sb.toString();
    }
}
