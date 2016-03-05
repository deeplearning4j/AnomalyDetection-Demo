package org.deeplearning4j.examples.data;

import lombok.Data;

/**
 * Created by Alex on 5/03/2016.
 */
@Data
public class DataAction {

    private final Transform transform;
    private final Filter filter;

    public DataAction(Transform transform){
        this.transform = transform;
        this.filter = null;
    }

    public DataAction(Filter filter){
        this.transform = null;
        this.filter = filter;
    }

}
