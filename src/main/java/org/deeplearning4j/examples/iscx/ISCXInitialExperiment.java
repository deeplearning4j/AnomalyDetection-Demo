package org.deeplearning4j.examples.iscx;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;

/**
 * Created by Alex on 8/03/2016.
 */
public class ISCXInitialExperiment {

    public static boolean isWin = true;

    public static final String windowsPath = "C:/Data/ISCX/labeled_flows_xml/TestbedMonJun14Flows.xml";  //Only a subset of the data

    public static final String DATA_PATH = (isWin ? windowsPath : null);

    public static void main(String[] args) throws Exception {


        ObjectMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);

        RootClass root = xmlMapper.readValue(new File(DATA_PATH),RootClass.class);


        for( int i=0; i<10; i++ ){
            System.out.println(root.elements[i]);
        }

    }

    @AllArgsConstructor(suppressConstructorProperties = true) @NoArgsConstructor @Data
    @JacksonXmlRootElement(localName = "dataroot")
    private static class RootClass {

        @JacksonXmlProperty(localName = "TestbedMonJun14Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements;
    }

    @Data
    @AllArgsConstructor(suppressConstructorProperties = true)
    @NoArgsConstructor
    private static class Element {

        //TODO: Not sure if this is a comprehensive set of fields or not

        @JacksonXmlProperty(localName = "appName")
        private String appName;

        @JacksonXmlProperty(localName = "totalSourceBytes")
        private String totalSourceBytes;

        @JacksonXmlProperty(localName = "totalDestinationBytes")
        private String totalDestinationBytes;

        @JacksonXmlProperty(localName = "totalDestinationPackets")
        private String totalDestinationPackets;

        @JacksonXmlProperty(localName = "totalSourcePackets")
        private String totalSourcePackets;

        @JacksonXmlProperty(localName = "sourcePayloadAsBase64")
        private String sourcePayloadAsBase64;

        @JacksonXmlProperty(localName = "destinationPayloadAsBase64")
        private String destinationPayloadAsBase64;

        @JacksonXmlProperty(localName = "direction")
        private String direction;

        @JacksonXmlProperty(localName = "sourceTCPFlagsDescription")
        private String sourceTCPFlagsDescription;

        @JacksonXmlProperty(localName = "destinationTCPFlagsDescription")
        private String destinationTCPFlagsDescription;

        @JacksonXmlProperty(localName = "source")
        private String source;

        @JacksonXmlProperty(localName = "protocolName")
        private String protocolName;

        @JacksonXmlProperty(localName = "destination")
        private String destination;

        @JacksonXmlProperty(localName = "destinationPort")
        private String destinationPort;

        @JacksonXmlProperty(localName = "startDateTime")
        private String startDateTime;

        @JacksonXmlProperty(localName = "stopDateTime")
        private String stopDateTime;

        @JacksonXmlProperty(localName = "Tag")
        private String Tag;
    }


}
