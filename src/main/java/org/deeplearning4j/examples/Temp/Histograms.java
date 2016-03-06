package org.deeplearning4j.examples.Temp;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.DefaultIntervalXYDataset;
import org.jfree.data.xy.IntervalXYDataset;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;

/**
 * Created by Alex on 6/03/2016.
 */
public class Histograms {

    public static void plot(double[] binEdges, long[] counts, String title) {

        JFreeChart chart = chart(binEdges,counts,title);

        JFrame frame = new JFrame();
        frame.add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    }

    private static JFreeChart chart(double[] binEdges, long[] counts, String title){

        //http://www.jfree.org/jfreechart/api/javadoc/org/jfree/data/xy/DefaultIntervalXYDataset.html
        DefaultIntervalXYDataset dataset = new DefaultIntervalXYDataset();
//        double[][] series = new double[counts.length][0];
        double[][] series = new double[6][counts.length];
        for( int i=0; i< counts.length; i++ ){
//            double startX = binEdges[i];
//            double endX = binEdges[i+1];
//            double x = (endX-startX)/2.0;
//            double startY = 0.0;
//            double endY = counts[i];
//            double y = endY;
//
//            series[i] = new double[]{x,startX,endX,y,startY,endY};

            double startX = binEdges[i];
            double endX = binEdges[i+1];
            double x = (endX-startX)/2.0;
            double startY = 0.0;
            double endY = counts[i];
            double y = endY;
            series[0][i] = x;
            series[1][i] = startX;
            series[2][i] = endX;
            series[3][i] = y;
            series[4][i] = startY;
            series[5][i] = endY;
        }
        dataset.addSeries("", series);


        JFreeChart chart = ChartFactory.createXYBarChart(
                title,  // chart title
                "X",       // domain axis label
                false,
                "Y",       // range axis label
                dataset,   // data
                PlotOrientation.VERTICAL,
                true,      // include legend
                true,
                false
        );

        return chart;
    }


    public static void exportHistogramImage(File file, double[] binEdges, long[] counts, String title, int widthPixels, int heightPixels) throws Exception {

        JFreeChart chart = chart(binEdges,counts,title);
        BufferedImage bImg = new BufferedImage(widthPixels,heightPixels,BufferedImage.TYPE_INT_RGB);
        Graphics2D g2d = (Graphics2D) bImg.getGraphics();

        Rectangle2D area = new Rectangle2D.Double(0,0,widthPixels,heightPixels);
        chart.draw(g2d, area);

        ImageIO.write(bImg, "png", file );
    }

}
