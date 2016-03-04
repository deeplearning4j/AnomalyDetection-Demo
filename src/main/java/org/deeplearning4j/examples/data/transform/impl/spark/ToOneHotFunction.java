package org.deeplearning4j.examples.data.transform.impl.spark;

import org.apache.spark.api.java.function.Function;
import org.canova.api.io.data.IntWritable;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.transform.ToOneHotTransform;

import java.util.*;

/**
 * Created by Alex on 4/03/2016.
 */
public class ToOneHotFunction implements Function<Collection<Writable>,Collection<Writable>> {

    //TODO: make this able to do multiple one-hot conversions in one go (much more efficient)
    private ToOneHotTransform transform;
    private Map<String,Integer> statesMap;

    public ToOneHotFunction(ToOneHotTransform transform) {
        this.transform = transform;
        List<String> stateNames = transform.getStateNames();
        this.statesMap = new HashMap<>(stateNames.size());
        for( int i=0; i<stateNames.size(); i++ ){
            this.statesMap.put(stateNames.get(i), i);
        }
    }

    @Override
    public Collection<Writable> call(Collection<Writable> writables) throws Exception {

        int idx = transform.getColumnIdx();

        int n = statesMap.size();
        List<Writable> out = new ArrayList<>(writables.size() + n);

        int i=0;
        for( Writable w : writables){

            if(i++ == idx){
                //Do conversion
                String str = w.toString();
                Integer classIdx = statesMap.get(str);
                if(classIdx == null) throw new RuntimeException("Unknown state (index not found): " + str);
                for( int j=0; j<n; j++ ){
                    if(j == classIdx ) out.add(new IntWritable(1));
                    else out.add(new IntWritable(0));
                }
            } else {
                //No change to this column
                out.add(w);
            }
        }
        return out;
    }
}
