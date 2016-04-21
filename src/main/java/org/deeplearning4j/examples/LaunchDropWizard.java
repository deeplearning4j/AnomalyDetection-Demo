package org.deeplearning4j.examples;

import org.deeplearning4j.examples.datasets.nb15.NB15Util;
import org.deeplearning4j.examples.datasets.nb15.ui.NB15TableConverter;
import org.deeplearning4j.examples.ui.TableConverter;
import org.deeplearning4j.examples.ui.UIDriver;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.ui.UiUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 21/04/2016.
 */
public class LaunchDropWizard {

    public static void main(String[] args){

        //Various settings required here
        Schema schema = NB15Util.getCsvSchema();
        List<String> labels = NB15Util.LABELS;
        List<String> services = NB15Util.SERVICES;
        int normalIdx = NB15Util.NORMALIDX;
        TableConverter tableConverter = new NB15TableConverter(schema);
        Map<String,Integer> columnMap = tableConverter.getColumnMap();
        UIDriver.createInstance(labels,normalIdx,services,tableConverter,columnMap);


        UiUtils.tryOpenBrowser("http://localhost:8080/intrusion/",null);

        System.out.println("Dropwizard: launch complete.");
    }
}
