package org.deeplearning4j.examples.dataProcessing.api.transform.string;

import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.ColumnType;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;
import org.deeplearning4j.examples.dataProcessing.api.metadata.CategoricalMetaData;
import org.deeplearning4j.examples.dataProcessing.api.metadata.ColumnMetaData;
import org.deeplearning4j.examples.dataProcessing.api.transform.BaseTransform;

import java.util.*;

/**
 * Convert a delimited String to a list of binary categorical columns.
 * Suppose the possible String values were {"a","b","c","d"} and the String column value to be converted contained
 * the String "a,c", then the 4 output columns would have values ["true","false","true","false"]
 *
 * @author Alex Black
 */
public class StringListToCategoricalSetTransform extends BaseTransform {

    private final String columnName;
    private final List<String> newColumnNames;
    private final List<String> categoryTokens;
    private final String delim;

    private final Map<String,Integer> map;

    private int columIdx = -1;

    /**
     *
     * @param columnName The name of the column to convert
     * @param newColumnNames The names of the new columns to create
     * @param categoryTokens The possible tokens that may be present. Note this list must have the same length and order
     *                       as the newColumnNames list
     * @param delim The delimiter for the Strings to convert
     */
    public StringListToCategoricalSetTransform(String columnName, List<String> newColumnNames, List<String> categoryTokens,
                                               String delim) {
        if(newColumnNames.size() !=  categoryTokens.size()) throw new IllegalArgumentException("Names/tokens sizes cannot differ");
        this.columnName = columnName;
        this.newColumnNames = newColumnNames;
        this.categoryTokens = categoryTokens;
        this.delim = delim;

        map = new HashMap<>();
        for( int i=0; i<categoryTokens.size(); i++ ){
            map.put(categoryTokens.get(i),i);
        }
    }

    @Override
    public Schema transform(Schema inputSchema) {

        int colIdx = inputSchema.getIndexOfColumn(columnName);

        List<ColumnMetaData> oldMeta = inputSchema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(oldMeta.size() + newColumnNames.size() - 1);
        List<String> oldNames = inputSchema.getColumnNames();
        List<String> newNames = new ArrayList<>(oldMeta.size() + newColumnNames.size() - 1);

        Iterator<ColumnMetaData> typesIter = oldMeta.iterator();
        Iterator<String> namesIter = oldNames.iterator();

        int i=0;
        while(typesIter.hasNext()){
            ColumnMetaData t = typesIter.next();
            String name = namesIter.next();
            if(i++ == colIdx){
                //Replace String column with a set of binary/categorical columns
                if(t.getColumnType() != ColumnType.String) throw new IllegalStateException("Cannot convert non-string type");

                for( int j=0; j<newColumnNames.size(); j++ ){
                    ColumnMetaData meta = new CategoricalMetaData("true","false");
                    newMeta.add(meta);
                    newNames.add(newColumnNames.get(j));
                }
            } else {
                newMeta.add(t);
                newNames.add(name);
            }
        }

        return inputSchema.newSchema(newNames, newMeta);

    }

    @Override
    public void setInputSchema(Schema inputSchema) {
        this.inputSchema = inputSchema;
        this.columIdx = inputSchema.getIndexOfColumn(columnName);
    }

    @Override
    public List<Writable> map(List<Writable> writables) {
        int n = writables.size();
        List<Writable> out = new ArrayList<>(n);

        int i=0;
        for(Writable w : writables){
            if(i++ == columIdx){
                String str = w.toString();
                boolean[] present = new boolean[categoryTokens.size()];
                if(str != null && !str.isEmpty()){
                    String[] split = str.split(delim);
                    for( String s : split){
                        Integer idx = map.get(s);
                        if(idx == null) throw new IllegalStateException("Encountered unknown String: \"" + s + "\"");
                        present[idx] = true;
                    }
                }
                for( int j=0; j<present.length; j++ ){
                    out.add(new Text( present[j] ? "true" : "false"));
                }
            } else {
                //No change to this column
                out.add(w);
            }
        }

        return out;
    }
}
