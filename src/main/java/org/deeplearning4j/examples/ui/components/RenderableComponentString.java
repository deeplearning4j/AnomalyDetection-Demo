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

@EqualsAndHashCode(callSuper = true)
@Data
public class RenderableComponentString extends RenderableComponent {
    public static final String COMPONENT_TYPE = "string";
    private String string;

    public RenderableComponentString(){
        super(COMPONENT_TYPE);
        //No arg constructor for Jackson deserialization
        string = null;
    }

    public RenderableComponentString(String string){
        super(COMPONENT_TYPE);
        this.string = string;
    }


    @Override
    public String toString(){
        return "RenderableComponentString(" + string + ")";
    }

}
