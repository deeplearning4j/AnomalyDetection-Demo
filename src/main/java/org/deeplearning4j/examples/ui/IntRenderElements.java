package org.deeplearning4j.examples.ui;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.deeplearning4j.examples.ui.components.RenderElements;

/**
 * Created by Alex on 26/03/2016.
 */
@AllArgsConstructor @NoArgsConstructor @Data
public class IntRenderElements {
    private int idx;
    private RenderElements renderElements;
}
