package org.deeplearning4j.examples.data.spark.analysis;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor
public class SelectColumnFunction implements Function<Collection<Writable>,Writable> {

    private final int column;

    @Override
    public Writable call(Collection<Writable> writables) throws Exception {
        if(writables instanceof List) return ((List<Writable>)writables).get(column);
        else {
            Iterator<Writable> it = writables.iterator();
            int count = 0;
            Writable w = null;
            while(count++ < column) w = it.next();
            if(w == null) throw new RuntimeException();
            return w;
        }
    }
}
