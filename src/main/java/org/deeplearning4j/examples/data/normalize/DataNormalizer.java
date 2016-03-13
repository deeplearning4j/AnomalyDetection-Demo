package org.deeplearning4j.examples.data.normalize;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.data.analysis.DataAnalysis;
import org.deeplearning4j.examples.data.transform.normalize.Normalize;

import java.util.Collection;
import java.util.Map;

/**
 * Created by Alex on 13/03/2016.
 */
public class DataNormalizer implements IDataNormalizer {

    private DataNormalizer(Builder builder){


    }


    @Override
    public Collection<Writable> normalize(Collection<Writable> data) {
        return null;
    }

    @Override
    public Collection<Collection<Writable>> normalizeSequence(Collection<Collection<Writable>> data) {
        return null;
    }


    public static class Builder {

        private Map<String,Normalize> map;

        public Builder normalize(String columnName, Normalize type){
            map.put(columnName,type);
            return this;
        }

    }

}
