package org.deeplearning4j.examples.ui2.NB15;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.util.Pair;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui2.StreamingUI;
import org.deeplearning4j.preprocessing.api.schema.Schema;
import org.deeplearning4j.ui.api.Component;
import org.deeplearning4j.ui.api.LengthUnit;
import org.deeplearning4j.ui.api.Style;
import org.deeplearning4j.ui.components.component.ComponentDiv;
import org.deeplearning4j.ui.components.component.style.StyleDiv;
import org.deeplearning4j.ui.components.table.ComponentTable;
import org.deeplearning4j.ui.components.table.style.StyleTable;
import org.deeplearning4j.ui.components.text.ComponentText;
import org.deeplearning4j.ui.components.text.style.StyleText;
import org.nd4j.linalg.api.ndarray.INDArray;
import scala.Tuple3;

import java.awt.*;
import java.util.*;
import java.util.List;

/**
 * Created by Alex on 8/04/2016.
 */
public class NB15StreamingUI extends StreamingUI {

    private static final int NORMAL_CLASS_IDX = 0;
    private static final int CHART_HISTORY_SECONDS = 20;
    private static final int NUM_ATTACKS_TO_KEEP = 22;

    private final int sdBytesCol;
    private final int dsBytesCol;
    private final int serviceCol;
    private final int sourceIpCol;
    private final int sourcePortCol;
    private final int destIpCol;
    private final int destPortCol;

    private long lastUpdateTime = 0;
    private LinkedList<Tuple3<Long,INDArray,List<Writable>>> lastAttacks = new LinkedList<>();

    private LinkedList<Pair<Long,Double>> connectionRateHistory = new LinkedList<>();
    private LinkedList<Pair<Long,Double>> byteRateHistory = new LinkedList<>();

    //Keep a small history here, to smooth out the instantaneous rate calculations (i.e., delta(connections_now - connections_t-3)) etc
    private LinkedList<Long> flowCountHistory = new LinkedList<>();
    private LinkedList<Long> updateTimeHistory = new LinkedList<>();
    private LinkedList<Double> sumBytesHistory = new LinkedList<>();
    private Map<String,LinkedList<Double>> serviceNamesHistory = new HashMap<>();

    public NB15StreamingUI(Schema rawDataSchema){

        sdBytesCol = rawDataSchema.getIndexOfColumn("source-dest bytes");
        dsBytesCol = rawDataSchema.getIndexOfColumn("source-dest bytes");
        serviceCol = rawDataSchema.getIndexOfColumn("service");
        sourceIpCol = rawDataSchema.getIndexOfColumn("source ip");
        sourcePortCol = rawDataSchema.getIndexOfColumn("source port");
        destIpCol = rawDataSchema.getIndexOfColumn("destination ip");
        destPortCol = rawDataSchema.getIndexOfColumn("destination port");
    }


    @Override
    public void processRecords(List<Tuple3<Long, INDArray, List<Writable>>> newRecords) {


        double sumBytes = 0.0;
        Map<String,Integer> serviceCounts = new HashMap<>();


        for(Tuple3<Long,INDArray,List<Writable>> t3 : newRecords){
            List<Writable> listWritables = t3._3();

            //Post the details to the web server:
            int idx = (int)((long)t3._1());

            if(sdBytesCol >= 0){
                try{
                    sumBytes += listWritables.get(sdBytesCol).toDouble();
                }catch(Exception e){ }
            }
            if(dsBytesCol >= 0){
                try{
                    sumBytes += listWritables.get(dsBytesCol).toDouble();
                }catch(Exception e){ }
            }

            String service = (serviceCol == -1 ? null : listWritables.get(serviceCol).toString());
            if(service != null){
                if(serviceCounts.containsKey(service)){
                    serviceCounts.put(service,serviceCounts.get(service)+1);
                } else {
                    serviceCounts.put(service,1);
                }
            }

            //Now: determine if this is an attack or not...
            float[] probs = t3._2().data().asFloat();
            if(probs[NORMAL_CLASS_IDX] < 0.5f){
                //Attack
                lastAttacks.add(t3);

//                //This appears to be a bottleneck at times... maybe ok if just attacks though...
//                RenderableComponent rc = tableConverter.rawDataToTable(c);
//                RenderableComponent barChart = new RenderableComponentHorizontalBarChart.Builder()
//                        .addValues(classNames,probs)
//                        .title("Network Predictions: Attack Type Probabilities")
//                        .margins(40,20,150,20)
//                        .xMin(0.0).xMax(1.0)
//                        .build();
//
//
//                RenderElements re = new RenderElements(rc,barChart);
//                renderElementsList.add(new IntRenderElements(idx,re));
            }
        }

        if(lastUpdateTime > 0){
            long pastUpdateTime = updateTimeHistory.getFirst();
            long pastCount = flowCountHistory.getFirst();
            long newCount = flowCountHistory.getLast() + newRecords.size();
            double connectionsPerSec = 1000.0 * (newCount - pastCount) / (System.currentTimeMillis() - pastUpdateTime);

            double pastSumBytes = sumBytesHistory.getFirst();
            double newSumBytes = sumBytesHistory.getLast() + sumBytes;
            double kBytesPerSec = 1000.0 * (newSumBytes - pastSumBytes) / ((System.currentTimeMillis() - pastUpdateTime) * 1024.0);
            //1000.0 is due to time being in MS, rate being in connections/sec

            lastUpdateTime = System.currentTimeMillis();

            //Add the new instantaneous rate:
            connectionRateHistory.add(new Pair<>(lastUpdateTime,connectionsPerSec));
            byteRateHistory.add(new Pair<>(lastUpdateTime,kBytesPerSec));
        } else {
            lastUpdateTime = System.currentTimeMillis();
        }

        flowCountHistory.addLast((flowCountHistory.size() > 0 ? flowCountHistory.getLast() + newRecords.size() : newRecords.size()));
        updateTimeHistory.addLast(lastUpdateTime);
        sumBytesHistory.addLast((sumBytesHistory.size() > 0 ? sumBytesHistory.getLast() + sumBytes : sumBytes));

        while(flowCountHistory.size() > 4 ) flowCountHistory.removeFirst();
        while(updateTimeHistory.size() > 4 ) updateTimeHistory.removeFirst();
        while(sumBytesHistory.size() > 4) sumBytesHistory.removeFirst();


        //Remove any old instantaneous rates (older than chart cutoff)
        Pair<Long,Double> last = (connectionRateHistory.isEmpty() ? null : connectionRateHistory.getFirst());
        long cutoff = (long)(lastUpdateTime - 1000.0*CHART_HISTORY_SECONDS);
        while(last != null && last.getFirst() < cutoff){
            connectionRateHistory.removeFirst();
            last = connectionRateHistory.getFirst();
        }
        last = (byteRateHistory.isEmpty() ? null : byteRateHistory.getFirst());
        while(last != null && last.getFirst() < cutoff){
            byteRateHistory.removeFirst();
            last = byteRateHistory.getFirst();
        }

        //Create the arrays for the charts
        double[] time = new double[connectionRateHistory.size()];
        double[] rate = new double[time.length];
        int i=0;
        for(Pair<Long,Double> p : connectionRateHistory){
            time[i] = (p.getFirst() - lastUpdateTime)/1000.0;
            rate[i++] = p.getSecond();
        }

        double[] bytesTime = new double[byteRateHistory.size()];
        double[] bytesRate = new double[byteRateHistory.size()];
        i=0;
        for(Pair<Long,Double> p : byteRateHistory){
            bytesTime[i] = (p.getFirst() - lastUpdateTime)/1000.0;
            bytesRate[i++] = p.getSecond();
        }

        //And post the instantaneous connection rate and bytes/sec charts...
//        RenderableComponent connectionRate = new RenderableComponentLineChart.Builder()
//                .setRemoveAxisHorizontal(true)
//                .legend(false)
//                .margins(30,20,60,20)
//                .addSeries("Connections/sec",time,rate).build();

//        RenderableComponent byteRate = new RenderableComponentLineChart.Builder()
//                .setRemoveAxisHorizontal(true)
//                .legend(false)
//                .margins(30,20,60,20)
//                .addSeries("kBytes/sec",bytesTime,bytesRate).build();



        //Now, post details of the last 20 attacks
        //For now: just post details of last 20 FLOWS, whether attacks or not
        while(lastAttacks.size() > NUM_ATTACKS_TO_KEEP) lastAttacks.removeFirst();

        String[][] table = new String[lastAttacks.size()][5];
        int j=0;
        for(Tuple3<Long,INDArray,List<Writable>> t3 : lastAttacks ){
            List<Writable> l = t3._3();
            float[] probs = t3._2().data().asFloat();
            int maxIdx = 0;
            for( int k=1; k<probs.length; k++ ){
                if(probs[maxIdx] < probs[k] ) maxIdx = k;
            }

            float attackProb = 100.0f * (1.0f - probs[NORMAL_CLASS_IDX]);
            String attackProbStr = (attackProb <1.0f ? "< 1%" : String.format("%.1f",attackProb) + "%");

            table[j][0] = String.valueOf(t3._1());
            table[j][1] = l.get(sourceIpCol) + " : " + l.get(sourcePortCol);
            table[j][2] = l.get(destIpCol) + " : " + l.get(destPortCol);
            table[j][3] = attackProbStr;
            table[j][4] = "todo";   //classNames.get(maxIdx);
            j++;
        }

        Component tableComp = new ComponentTable.Builder(new StyleTable.Builder()
                    .borderWidth(1).columnWidths(LengthUnit.Px,40,200,200,100,100).build())
                .header("#","Source","Destination","Attack Prob.","Type")
                .content(table)
                .build();



//        RenderableComponentTable rct = new RenderableComponentTable.Builder()
//                .header("#","Source","Destination","Attack Prob.","Type")
//                .table(table)
//                .paddingPx(5,5,0,0)
//                .border(1)
//                .backgroundColor("#FFFFFF")
//                .headerColor("#CCCCCC")
//                .colWidthsPercent(8,28,28,16,20)
//                .build();

//        //Calculate new proportions:
//        RenderableComponentStackedAreaChart.Builder rcArea = new RenderableComponentStackedAreaChart.Builder()
//                .title("title")
//                .setRemoveAxisHorizontal(true)
//                .margins(30,20,60,20)
//                .setXValues(time);
//
//        double alpha = 0.9;
//        double sum = 0.0;
//        double[] props = new double[serviceNames.size()];
//        int k=0;
//        for(String s : serviceNames){
//            LinkedList<Double> history = serviceNamesHistory.get(s);
//            double lastProp;
//            if(history == null){
//                history = new LinkedList<>();
//                serviceNamesHistory.put(s,history);
//                lastProp = 0.0;
//            } else if(history.size() == 0) {
//                lastProp = 0.0;
//            } else {
//                lastProp = history.getLast();
//            }
//
//            int count = (serviceCounts.containsKey(s) ? serviceCounts.get(s) : 0);
//            double rawProportion = ((double)count)/list.size();
//
//            double newProp = alpha * lastProp + (1.0-alpha)*rawProportion;
//            props[k++] = newProp;
//            sum += newProp;
//
//            while(history.size() > time.length) history.removeFirst();
//        }
//        for(k=0; k<props.length; k++ ){
//            props[k] /= sum;
//        }
//
//        k=0;
//        for(String s : serviceNames){
//            LinkedList<Double> history = serviceNamesHistory.get(s);
//            history.addLast(props[k++]);
//
//            while(history.size() > time.length) history.removeFirst();
//
//            double[] out = new double[time.length];
//            for( int l=0; l<out.length; l++ ){
//                out[l] = history.get(l);
//            }
//            rcArea.addSeries(s,out);
//        }

//        Style topDivStyle = new StyleDiv.Builder()
//                .backgroundColor(Color.GRAY)
//                .width(33.333, LengthUnit.Percent)
//                .floatValue(StyleDiv.FloatValue.left)
//                .build();
//
//        Style headerDivStyle = new StyleDiv.Builder()
//                .backgroundColor(Color.BLACK)
//                .width(100,LengthUnit.Percent)
//                .build();
//
//        StyleText headerStyle = new StyleText.Builder()
//                .color(Color.WHITE)
//                .fontSize(18)
//                .build();
//        Component text = new ComponentText("Network Utilization: Connections/sec", headerStyle);
//
//        ComponentDiv headerDiv1 = new ComponentDiv(headerDivStyle,text);
//
//        Component topThird1 = new ComponentDiv(topDivStyle, headerDiv1);

        List<Component> list = new ArrayList<>();
        list.add(getHeader());

        list.add(tableComp);

        updateUI(list.toArray(new Component[list.size()]));

        System.out.println("***** CALLED UPDATEUI *****");
    }

    private Component getHeader(){

        Style topDivStyle = new StyleDiv.Builder()
                .backgroundColor(Color.GRAY)
                .width(33.333, LengthUnit.Percent)
                .floatValue(StyleDiv.FloatValue.left)
                .build();

        Style headerDivStyle = new StyleDiv.Builder()
                .backgroundColor(Color.BLACK)
                .width(100,LengthUnit.Percent)
                .build();

        StyleText headerStyle = new StyleText.Builder()
                .color(Color.WHITE)
                .fontSize(16)
                .build();

        ComponentDiv headerDiv1 = new ComponentDiv(headerDivStyle,new ComponentText("Network Utilization: Connections/sec", headerStyle));
        ComponentDiv headerDiv2 = new ComponentDiv(headerDivStyle,new ComponentText("Middle", headerStyle));
        ComponentDiv headerDiv3 = new ComponentDiv(headerDivStyle,new ComponentText("Right", headerStyle));

        Component topThird1 = new ComponentDiv(topDivStyle, headerDiv1);
        Component topThird2 = new ComponentDiv(topDivStyle, headerDiv2);
        Component topThird3 = new ComponentDiv(topDivStyle, headerDiv3);

        Component div = new ComponentDiv(null,topThird1,topThird2,topThird3);

        return div;
    }
}
