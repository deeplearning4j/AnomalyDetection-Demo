package org.deeplearning4j.examples;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by Alex on 21/04/2016.
 */
public class PrintClassPath {

    public static void main(String[] args){

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }
    }
}
