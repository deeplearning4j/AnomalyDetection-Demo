package org.deeplearning4j.examples.misc;

import lombok.AllArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.canova.api.writable.Writable;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 7/03/2016.
 */
public class SparkExport {

    //Quick and dirty CSV export (using Spark). Eventually, rework this to use Canova record writers on Spark
    public static void exportCSVSpark(String directory, String delimiter, int outputSplits, JavaRDD<Collection<Writable>> data, JavaSparkContext sc){

        JavaRDD<String> lines = data.map(new ToStringFunction(delimiter));
        lines.coalesce(outputSplits);

        lines.saveAsTextFile(directory);
    }

    //Another quick and dirty CSV export (local). Dumps all values into a single file
    public static void exportCSVLocal(File outputFile, String delimiter, JavaRDD<Collection<Writable>> data) throws Exception {

        JavaRDD<String> lines = data.map(new ToStringFunction(delimiter));
        List<String> linesList = lines.collect();   //Requires all data in memory

        FileUtils.writeLines(outputFile, linesList);
    }

    //Another quick and dirty CSV export (local). Dumps all values into a single file
    public static void exportCSVLocal(String outputDir, String baseFileName, int numFiles, String delimiter,
                                      JavaRDD<Collection<Writable>> data) throws Exception {

        JavaRDD<String> lines = data.map(new ToStringFunction(delimiter));
        double[] split = new double[numFiles];
        for( int i=0; i<split.length; i++ ) split[i] = 1.0 / numFiles;
        JavaRDD<String>[] splitData = lines.randomSplit(split);

        int count = 0;
        for(JavaRDD<String> subset : splitData){
            String path = FilenameUtils.concat(outputDir,baseFileName + (count++) + ".csv");
            List<String> linesList = subset.collect();
            FileUtils.writeLines(new File(path),linesList);
        }
    }



    @AllArgsConstructor
    private static class ToStringFunction implements Function<Collection<Writable>,String> {

        private final String delim;

        @Override
        public String call(Collection<Writable> c) throws Exception {

            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for(Writable w : c){
                if(!first) sb.append(delim);
                sb.append(w.toString());
                first = false;
            }

            return sb.toString();
        }
    }

}