package org.deeplearning4j.examples.data.analysis.columns;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Alex on 4/03/2016.
 */
@AllArgsConstructor @Data
public class BlobAnalysis implements ColumnAnalysis {


    @Override
    public String toString(){
        return "BlobAnalysis()";
    }

}