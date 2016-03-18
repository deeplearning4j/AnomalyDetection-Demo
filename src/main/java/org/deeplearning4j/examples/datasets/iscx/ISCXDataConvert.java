package org.deeplearning4j.examples.datasets.iscx;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.utils.DataPathUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 8/03/2016.
 */
public class ISCXDataConvert {

    public static final String[] fileNames = {
            "TestbedMonJun14Flows.xml",
            "TestbedSatJun12Flows.xml",
            "TestbedSunJun13Flows.xml",
            "TestbedThuJun17-1Flows.xml",
            "TestbedThuJun17-2Flows.xml",
            "TestbedThuJun17-3Flows.xml",
            "TestbedTueJun15-1Flows.xml",
            "TestbedTueJun15-2Flows.xml",
            "TestbedTueJun15-3Flows.xml",
            "TestbedWedJun16-1Flows.xml",
            "TestbedWedJun16-2Flows.xml",
            "TestbedWedJun16-3Flows.xml"};


    protected static String dataSet = "ISCX";
    protected static final DataPathUtil PATH = new DataPathUtil(dataSet);
    public static final String IN_DIRECTORY = PATH.RAW_DIR;
    public static final String OUT_DIRECTORY = PATH.IN_DIR;

    public static void main(String[] args) throws Exception {


        //Read, parse, convert to CSV and write to new file
        ObjectMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);




        for( int i=0; i<fileNames.length; i++ ) {
            String thisPath = FilenameUtils.concat(IN_DIRECTORY, fileNames[i]);
//            RootClass root = xmlMapper.readValue(new File(DATA_DIR,fileNames[i]),RootClass.class);
            String fileAsString = FileUtils.readFileToString(new File(thisPath));
            fileAsString = fileAsString.replaceAll("&([^;]+(?!(?:\\w|;)))", "&amp;$1"); //Some of the files contain invalid (non-escaped) '&' characters
            fileAsString = fileAsString.replaceAll(String.valueOf('\u0000'),"");    //And others contain invalid characters (unicode character number 0) that isn't valid (and can't be escaped in xml) //http://stackoverflow.com/questions/730133/invalid-characters-in-xml

            RootClass root = xmlMapper.readValue(fileAsString,RootClass.class);

            StringBuilder sb = new StringBuilder();

            System.out.println(thisPath);
            Element[] elements = root.getElements(i);
            for (int j = 0; j < elements.length; j++) {
                if(j != 0) sb.append("\n");
                sb.append(writablesToString(elements[j].asWritables(),","));
            }

            String newFileName = fileNames[i].substring(0,fileNames[i].length()-3) + "csv";
            String outputFile = FilenameUtils.concat(OUT_DIRECTORY,newFileName);
            FileUtils.writeStringToFile(new File(outputFile),sb.toString());
        }
    }

    public static String writablesToString(Collection<Writable> c, String delim) throws Exception {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(Writable w : c){
            if(!first) sb.append(delim);
            sb.append(w.toString());
            first = false;
        }

        return sb.toString();
    }

    public List<List<Writable>> loadData(int i) throws Exception {

        ObjectMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);

        String thisPath = FilenameUtils.concat(IN_DIRECTORY, fileNames[i]);
//            RootClass root = xmlMapper.readValue(new File(DATA_DIR,fileNames[i]),RootClass.class);
        String fileAsString = FileUtils.readFileToString(new File(thisPath));
        fileAsString = fileAsString.replaceAll("&([^;]+(?!(?:\\w|;)))", "&amp;$1"); //Some of the files contain invalid (non-escaped) '&' characters
        fileAsString = fileAsString.replaceAll(String.valueOf('\u0000'),"");    //And others contain invalid characters (unicode character number 0) that isn't valid (and can't be escaped in xml) //http://stackoverflow.com/questions/730133/invalid-characters-in-xml

        RootClass root = xmlMapper.readValue(fileAsString, RootClass.class);

        Element[] elements = root.getElements(i);

        List<List<Writable>> out = new ArrayList<>(elements.length);
        for(Element e : elements){
            out.add(e.asWritables());
        }

        return out;
    }

    @AllArgsConstructor(suppressConstructorProperties = true) @NoArgsConstructor @Data
    @JacksonXmlRootElement(localName = "dataroot")
    private static class RootClass {

        @JacksonXmlProperty(localName = "TestbedMonJun14Flows") //For TestbedMonJun14Flows.xml
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements;

        @JacksonXmlProperty(localName = "TestbedSatJun12")      //For TestbedSatJun12Flows.xml... and yes, this shouldn't end with "Flows"
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements1;

        @JacksonXmlProperty(localName = "TestbedSunJun13Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements2;


        @JacksonXmlProperty(localName = "TestbedThuJun17-1Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements3;

        @JacksonXmlProperty(localName = "TestbedThuJun17-2Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements4;

        @JacksonXmlProperty(localName = "TestbedThuJun17-3Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements5;

        @JacksonXmlProperty(localName = "TestbedTueJun15-1Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements6;

        @JacksonXmlProperty(localName = "TestbedTueJun15-2Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements7;

        @JacksonXmlProperty(localName = "TestbedTueJun15-3Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements8;

        @JacksonXmlProperty(localName = "TestbedWedJun16-1Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements9;

        @JacksonXmlProperty(localName = "TestbedWedJun16-2Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements10;

        @JacksonXmlProperty(localName = "TestbedWedJun16-3Flows")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Element[] elements11;


        public Element[] getElements(int i){
            switch(i){
                case 0: return elements;
                case 1: return elements1;
                case 2: return elements2;
                case 3: return elements3;
                case 4: return elements4;
                case 5: return elements5;
                case 6: return elements6;
                case 7: return elements7;
                case 8: return elements8;
                case 9: return elements9;
                case 10: return elements10;
                case 11: return elements11;
                default: throw new RuntimeException();
            }
        }
    }

    @Data
    @AllArgsConstructor(suppressConstructorProperties = true)
    @NoArgsConstructor
    private static class Element {
        //Accoding to the xml schema (TestbedMonJun14Flows.xsd) this should be all of the fields

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

        //UTF is annoying, as we can't encode it as a CSV (because it could contain commas, etc)
//        @JacksonXmlProperty(localName = "destinationPayloadAsUTF")
//        private String destinationPayloadAsUTF;

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


        public List<Writable> asWritables(){
            List<Writable> list = new ArrayList<>(17);
            list.add(new Text(appName == null ? "" : appName));
            list.add(new Text(totalSourceBytes == null ? "" : totalSourceBytes));
            list.add(new Text(totalDestinationBytes == null ? "" : totalDestinationBytes));
            list.add(new Text(totalDestinationPackets == null ? "" : totalDestinationPackets));
            list.add(new Text(totalSourcePackets == null ? "" : totalSourcePackets));
            list.add(new Text(sourcePayloadAsBase64 == null ? "" : sourcePayloadAsBase64));
            list.add(new Text(destinationPayloadAsBase64  == null ? "" : destinationPayloadAsBase64));
            list.add(new Text(direction == null ? "" : direction));
            list.add(new Text(sourceTCPFlagsDescription == null ? "" : sourceTCPFlagsDescription.replaceAll(",",";")));     //Replace comma delim for export to CSV
            list.add(new Text(destinationTCPFlagsDescription == null ? "" : destinationTCPFlagsDescription.replaceAll(",",";")));   //as above
            list.add(new Text(source == null ? "" : source));
            list.add(new Text(protocolName == null ? "" : protocolName));
            list.add(new Text(destination == null ? "" : destination));
            list.add(new Text(destinationPort == null ? "" : destinationPort));
            list.add(new Text(startDateTime == null ? "" : startDateTime));
            list.add(new Text(stopDateTime == null ? "" : stopDateTime));
            list.add(new Text(Tag == null ? "" : Tag));
            return list;
        }
    }


}
