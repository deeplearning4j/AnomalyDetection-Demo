/*
 *
 *  * Copyright 2016 Skymind,Inc.
 *  *
 *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *    you may not use this file except in compliance with the License.
 *  *    You may obtain a copy of the License at
 *  *
 *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *    Unless required by applicable law or agreed to in writing, software
 *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *    See the License for the specific language governing permissions and
 *  *    limitations under the License.
 *
 */
package org.deeplearning4j.examples.ui.components;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class RenderableComponentHorizontalBarChart extends RenderableComponent {
    public static final String COMPONENT_TYPE = "horizontalbarchart";

    private String title;
    private List<String> labels = new ArrayList<>();
    private List<Double> values = new ArrayList<>();
    private int marginTop;
    private int marginBottom;
    private int marginLeft;
    private int marginRight;
    private Double xmin;
    private Double xmax;

    private RenderableComponentHorizontalBarChart(Builder builder){
        super(COMPONENT_TYPE);
        title = builder.title;
        labels = builder.labels;
        values = builder.values;
        this.marginTop = builder.marginTop;
        this.marginBottom = builder.marginBottom;
        this.marginLeft = builder.marginLeft;
        this.marginRight = builder.marginRight;
        this.xmin = builder.xMin;
        this.xmax = builder.xMax;
    }

    public RenderableComponentHorizontalBarChart(){
        super(COMPONENT_TYPE);
        //no-arg constructor for Jackson
    }



    public static class Builder {

        private String title;
        private List<String> labels = new ArrayList<>();
        private List<Double> values = new ArrayList<>();
        private int marginTop = 60;
        private int marginBottom = 60;
        private int marginLeft = 60;
        private int marginRight = 20;
        private Double xMin;
        private Double xMax;

        public Builder title(String title){
            this.title = title;
            return this;
        }

        public Builder addValue(String name, double value){
            labels.add(name);
            values.add(value);
            return this;
        }

        public Builder addValues(List<String> names, double[] values){
            for( int i=0; i<names.size(); i++ ){
                addValue(names.get(i),values[i]);
            }
            return this;
        }

        public Builder margins(int top, int bottom, int left, int right){
            this.marginTop = top;
            this.marginBottom = bottom;
            this.marginLeft = left;
            this.marginRight = right;
            return this;
        }

        public Builder xMin(double xMin){
            this.xMin = xMin;
            return this;
        }

        public Builder xMax(double xMax){
            this.xMax = xMax;
            return this;
        }

        public Builder addValues(List<String> names, float[] values){
            for( int i=0; i<names.size(); i++ ){
                addValue(names.get(i),values[i]);
            }
            return this;
        }

        public RenderableComponentHorizontalBarChart build(){
            return new RenderableComponentHorizontalBarChart(this);
        }

    }

}
