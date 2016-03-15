package org.deeplearning4j.examples.ui;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import io.dropwizard.views.ViewMessageBodyWriter;
import org.canova.api.berkeley.Pair;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.components.*;
import org.deeplearning4j.examples.ui.config.NIDSConfig;
import org.deeplearning4j.examples.ui.resources.FlowDetailsResource;
import org.deeplearning4j.examples.ui.resources.LineChartResource;
import org.deeplearning4j.examples.ui.resources.TableResource;
import org.deeplearning4j.examples.ui.resources.UIResource;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Alex on 14/03/2016.
 */
public class UIDriver extends Application<NIDSConfig> {

    public static final double CHART_HISTORY_SECONDS = 20.0;   //N seconds of chart history for UI

    private static volatile UIDriver instance;

    private static final Logger log = LoggerFactory.getLogger(UIDriver.class);

    private static TableConverter tableConverter;
    private static Map<String,Integer> columnsMap;
    private static List<String> classNames;
    private static int normalClassIdx;

    private LinkedBlockingQueue<Tuple3<Long,INDArray,Collection<Writable>>> predictions = new LinkedBlockingQueue<>();
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    //Web targets: for posting results
    private final Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
    private final WebTarget connectionRateChartTarget = client.target("http://localhost:8080/charts/update/connection");
    private final WebTarget bytesRateChartTarget = client.target("http://localhost:8080/charts/update/bytes");
    private final WebTarget tableTarget = client.target("http://localhost:8080/table/update");

    private Thread uiThread;

    private UIDriver(){
        super();

        try{
            run("server", "dropwizard.yml");
        }catch(Exception e){
            throw new RuntimeException(e);
        }

//        ViewMessageBodyWriter

        uiThread = new Thread(new UIThreadRunnable());
        uiThread.setDaemon(true);
        uiThread.start();

//        log.info("*** UIDriver started ***");
        System.out.println("*** UIDriver started ***");
    }

    @Override
    public String getName() {
        return "nids-ui";
    }

    @Override
    public void initialize(Bootstrap<NIDSConfig> bootstrap) {
        bootstrap.addBundle(new ViewBundle<NIDSConfig>());
        bootstrap.addBundle(new AssetsBundle());
    }

    @Override
    public void run(NIDSConfig configuration, Environment environment) {
        final UIResource resource = new UIResource();
        environment.jersey().register(resource);

        //Register our resources
        environment.jersey().register(new FlowDetailsResource());
        environment.jersey().register(new LineChartResource());
        environment.jersey().register(new TableResource());

//        log.info("*** UIDriver run called ***");
        System.out.println("*** UIDriver run called ***");
    }

    /** Create a UIDriver if none exists, or get the existing one if not */
    public static UIDriver getInstance(){
        if(instance != null) return instance;

        //avoid race conditions on creation
        synchronized(UIDriver.class){
            if(instance != null) return instance;

            instance = new UIDriver();
            return instance;
        }
    }

    public static void setTableConverter(TableConverter tableConverter){
        UIDriver.tableConverter = tableConverter;
    }

    /**Columns map: i.e., what columns indexes for:
     * "source-dest bytes", "dest-source bytes" for
     *
     */
    public static void setColumnsMap(Map<String,Integer> map){
        UIDriver.columnsMap = map;
    }

    public static void setClassNames(List<String> classNames){
        UIDriver.classNames = classNames;
    }

    public static void setNormalClassIdx(int normalClassIdx){
        UIDriver.normalClassIdx = normalClassIdx;
    }

    public void addPrediction(Tuple3<Long,INDArray,Collection<Writable>> prediction){
        this.predictions.add(prediction);
    }

    public void addPredictions(List<Tuple3<Long,INDArray,Collection<Writable>>> predictions){
        this.predictions.addAll(predictions);
        System.out.println("************ ADDED " + predictions.size() + " - TOTAL = " + this.predictions.size() + " **********");
    }

    public void shutdown(){
        shutdown.set(true);
    }


    private class UIThreadRunnable implements Runnable {

        private long lastUpdateTime = 0;
        private LinkedList<Pair<Long,Double>> connectionRateHistory = new LinkedList<>();
        private LinkedList<Pair<Long,Double>> byteRateHistory = new LinkedList<>();
        private LinkedList<Tuple3<Long,INDArray,Collection<Writable>>> lastAttacks = new LinkedList<>();

        //Keep a small history here, to smooth out the instantaneous rate calculations (i.e., delta(connections_now - connections_t-3)) etc
        private LinkedList<Long> flowCountHistory = new LinkedList<>();
        private LinkedList<Long> updateTimeHistory = new LinkedList<>();
        private LinkedList<Double> sumBytesHistory = new LinkedList<>();

        @Override
        public void run() {
            try{
                runHelper();
            }catch(Exception e){
                //To catch any unchecked exceptions
                e.printStackTrace();
            }
        }

        private void runHelper(){
            log.info("Starting UI driver thread");

            List<Tuple3<Long,INDArray,Collection<Writable>>> list = new ArrayList<>(100);
            while(!shutdown.get()){

                try{
                    list.add(predictions.take());   //Blocks if no elements are available
                }catch(InterruptedException e){
                    log.warn("Interrupted exception thrown in UI driver thread");
                }
                predictions.drainTo(list);      //Doesn't block, but retrieves + removes all elements

                //Do something with the list...
                double sumBytes = 0.0;
                int sdBytesCol = (columnsMap.containsKey("source-dest bytes") ? columnsMap.get("source-dest bytes") : -1);
                int dsBytesCol = (columnsMap.containsKey("dest-source bytes") ? columnsMap.get("dest-source bytes") : -1);
                for(Tuple3<Long,INDArray,Collection<Writable>> t3 : list){
//                    System.out.println(t3);
                    Collection<Writable> c = t3._3();
                    List<Writable> listWritables = (c instanceof List ? ((List<Writable>)c) : new ArrayList<>(c));

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

                    //Now: determine if this is an attack or not...
                    float[] probs = t3._2().data().asFloat();
                    if(probs[normalClassIdx] < 0.5f){
                        //Attack
                        lastAttacks.add(t3);

                        //This appears to be a bottleneck at times... maybe ok if just attacks though...
                        RenderableComponent rc = tableConverter.rawDataToTable(c);
                        RenderableComponent barChart = new RenderableComponentHorizontalBarChart.Builder()
                                .addValues(classNames,probs)
                                .margins(10,20,150,20)
                                .xMin(0.0).xMax(1.0)
                                .build();


                        RenderElements re = new RenderElements(rc,barChart);
                        WebTarget wt = client.target("http://localhost:8080/flow/update/" + idx);
                        wt.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                                .post(Entity.entity(re,MediaType.APPLICATION_JSON));
                        
                    }
                }


                //Calculate the rate of updates (connections/sec). Just do delta connections / delta time for now
//                if(lastUpdateTime > 0){
//                    double connectionsPerSec = 1000.0 * list.size() / (System.currentTimeMillis() - lastUpdateTime);
////                    double bytesPerSec = 1000.0 * sumBytes / (System.currentTimeMillis() - lastUpdateTime);
//                    double kBytesPerSec = 1000.0 * sumBytes / ((System.currentTimeMillis() - lastUpdateTime) * 1024.0);
//                    //1000.0 is due to time being in MS, rate being in connections/sec
//
//                    lastUpdateTime = System.currentTimeMillis();
//
//                    //Add the new instantaneous rate:
//                    connectionRateHistory.add(new Pair<>(lastUpdateTime,connectionsPerSec));
//                    byteRateHistory.add(new Pair<>(lastUpdateTime,kBytesPerSec));
//                } else {
//                    lastUpdateTime = System.currentTimeMillis();
//                }

                if(lastUpdateTime > 0){
                    long pastUpdateTime = updateTimeHistory.getFirst();
                    long pastCount = flowCountHistory.getFirst();
                    long newCount = flowCountHistory.getLast() + list.size();
                    double connectionsPerSec = 1000.0 * (newCount - pastCount) / (System.currentTimeMillis() - pastUpdateTime);
//                    double bytesPerSec = 1000.0 * sumBytes / (System.currentTimeMillis() - lastUpdateTime);

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

                flowCountHistory.addLast((flowCountHistory.size() > 0 ? flowCountHistory.getLast() + list.size() : list.size()));
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
                RenderableComponent connectionRate = new RenderableComponentLineChart.Builder()
                        .addSeries("Connections/sec",time,rate).build();

                connectionRateChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(connectionRate,MediaType.APPLICATION_JSON));

                RenderableComponent byteRate = new RenderableComponentLineChart.Builder()
                        .addSeries("kBytes/sec",bytesTime,bytesRate).build();

                bytesRateChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(byteRate,MediaType.APPLICATION_JSON));



                //Now, post details of the last 20 attacks
                //For now: just post details of last 20 FLOWS, whether attacks or not
                while(lastAttacks.size() > 20) lastAttacks.removeFirst();

                String[][] table = new String[lastAttacks.size()][5];
                int j=0;
                for(Tuple3<Long,INDArray,Collection<Writable>> t3 : lastAttacks ){
                    List<Writable> l = (t3._3() instanceof List ? ((List<Writable>)t3._3()) : new ArrayList<>(t3._3()));
                    float[] probs = t3._2().data().asFloat();
                    int maxIdx = 0;
                    for( int k=1; k<probs.length; k++ ){
                        if(probs[maxIdx] < probs[k] ) maxIdx = k;
                    }

                    float attackProb = 100.0f * (1.0f - probs[normalClassIdx]);
                    String attackProbStr = (attackProb <1.0f ? "< 1%" : String.format("%.1f",attackProb) + "%");

                    table[j][0] = String.valueOf(t3._1());
                    table[j][1] = l.get(columnsMap.get("source ip")) + " : " + l.get(columnsMap.get("source port"));
                    table[j][2] = l.get(columnsMap.get("destination ip")) + " : " + l.get(columnsMap.get("destination port"));
                    table[j][3] = attackProbStr;
                    table[j][4] = classNames.get(maxIdx);
                    j++;
                }

                RenderableComponentTable rct = new RenderableComponentTable.Builder()
                        .header("#","Source","Destination","Attack Prob.","Type")
                        .table(table)
                        .paddingPx(4)
                        .border(1)
                        .colWidthsPercent(8,28,28,16,20)
                        .build();

                tableTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(rct,MediaType.APPLICATION_JSON));


                //Clear the list for the next iteration
                list.clear();
            }

            log.info("UI driver thread shutting down");
        }
    }

}