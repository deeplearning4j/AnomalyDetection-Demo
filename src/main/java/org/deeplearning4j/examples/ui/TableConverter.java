package org.deeplearning4j.examples.ui;

import org.datavec.api.writable.Writable;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;

import java.util.Collection;
import java.util.Map;

/**
 * Created by Alex on 14/03/2016.
 */
public interface TableConverter {

    RenderableComponentTable rawDataToTable(Collection<Writable> writables);

    Map<String,Integer> getColumnMap();
}
