package org.deeplearning4j.examples.ui.components;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 15/03/2016.
 */
@Data
public class RenderableComponentStackedAreaChart extends RenderableComponent{

    public static final String COMPONENT_TYPE = "stackedareachart";

    private String title;
    private List<double[]> x = new ArrayList<>();
    private List<double[]> y = new ArrayList<>();
    private List<String> labels = new ArrayList<>();
    private boolean normalize;

    public RenderableComponentStackedAreaChart(){
        super(COMPONENT_TYPE);
    }

    public RenderableComponentStackedAreaChart(Builder builder){
        super(COMPONENT_TYPE);

        this.title = builder.title;
        this.x = builder.x;
        this.y = builder.y;
        this.labels = builder.seriesNames;
        this.normalize = normalize;
    }

    public static class Builder {

        private String title;
        private List<double[]> x = new ArrayList<>();
        private List<double[]> y = new ArrayList<>();
        private List<String> seriesNames = new ArrayList<>();
        private boolean normalize = false;

        public Builder title(String title){
            this.title = title;
            return this;
        }

        public Builder addSeries(String seriesName, double[] xValues, double[] yValues){
            x.add(xValues);
            y.add(yValues);
            seriesNames.add(seriesName);
            return this;
        }

        public Builder normalize(boolean normalize){
            this.normalize = normalize;
            return this;
        }

        public RenderableComponentStackedAreaChart build(){
            return new RenderableComponentStackedAreaChart(this);
        }

    }

}
