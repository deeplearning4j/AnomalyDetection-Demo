package org.deeplearning4j.examples.data.transform.impl.spark;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.transform.RemoveColumnsTransform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@AllArgsConstructor
public class RemoveColumnsFunction implements Function<Collection<Writable>,Collection<Writable>> {

    public RemoveColumnsTransform transformation;

    @Override
    public Collection<Writable> call(Collection<Writable> writables) throws Exception {

        Set<Integer> columnsToRemove = transformation.getIndicesToRemove();

        List<Writable> outList = new ArrayList<>(writables.size()-columnsToRemove.size());

        int i=0;
        for(Writable w : writables){
            if(columnsToRemove.contains(i++)) continue;
            outList.add(w);
        }
        return outList;
    }
}
