package org.deeplearning4j.examples.ui;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;

import java.util.Collection;

/**
 * Created by Alex on 14/03/2016.
 */
public interface TableConverter {

    RenderableComponentTable rawDataToTable(Collection<Writable> writables);

}
