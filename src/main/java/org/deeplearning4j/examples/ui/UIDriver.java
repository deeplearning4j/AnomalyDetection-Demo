package org.deeplearning4j.examples.ui;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.components.*;
import org.deeplearning4j.examples.ui.config.NIDSConfig;
import org.deeplearning4j.examples.ui.resources.*;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**UIDriver: main class for the UI
 * Assumptions with current design:
 * -
 * How to use: Start UI using
 */
public class UIDriver extends Application<NIDSConfig> {

    public static final double CHART_HISTORY_SECONDS = 20.0;   //N seconds of chart history for UI
    private static final int NUM_ATTACKS_TO_KEEP = 22;

    private static volatile UIDriver instance;

    private static final Logger log = LoggerFactory.getLogger(UIDriver.class);

    private TableConverter tableConverter;
    private Map<String,Integer> columnsMap;
    private List<String> classNames;
    private List<String> serviceNames;
    private int normalClassIdx;

    private LinkedBlockingQueue<Tuple3<Long,INDArray,List<Writable>>> predictions = new LinkedBlockingQueue<>();
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    //Web targets: for posting results
    private final Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
    private final WebTarget connectionRateChartTarget = client.target("http://localhost:8080/charts/update/connection");
    private final WebTarget bytesRateChartTarget = client.target("http://localhost:8080/charts/update/bytes");
    private final WebTarget tableTarget = client.target("http://localhost:8080/table/update");
    private final WebTarget areaChartTarget = client.target("http://localhost:8080/areachart/update");

    private Thread uiThread;

    private UIDriver(List<String> classNames, int normalClassIdx, List<String> serviceNames,
                     TableConverter tableConverter, Map<String,Integer> columnsMap) {
        super();

        this.classNames = classNames;
        this.normalClassIdx = normalClassIdx;
        this.serviceNames = serviceNames;
        this.tableConverter = tableConverter;
        this.columnsMap = columnsMap;

        File config;
        try {
            config = new ClassPathResource("nids-dropwizard.yml").getFile();
//            System.out.println(FileUtils.readFileToString(config));
        } catch(Exception e){
            throw new RuntimeException(e);
        }

//        System.out.println("Dropwizard config: " + config.getAbsolutePath());


        try {
//            run("server", "dropwizard.yml");
            run("server", config.getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //Create UI thread
        uiThread = new Thread(new UIThreadRunnable());
        uiThread.setDaemon(true);
        uiThread.start();

        log.info("UIDriver started: http://localhost:8080/intrusion/");
    }

    @Override
    public String getName() {
        return "nids-ui";
    }

    @Override
    public void initialize(Bootstrap<NIDSConfig> bootstrap) {
        //Necessary to avoid abstract method issue with certain dependencies/versions of dropwizard
        bootstrap.addBundle(new ViewBundle<NIDSConfig>() {
            @Override
            public Map<String, Map<String, String>> getViewConfiguration(NIDSConfig nidsConfig) {
                return ImmutableMap.of();
            }
        });
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
        environment.jersey().register(new AreaChartResource());
    }

    /** Get an existing UIDriver instance. If no instance has been manually created, this will throw an exception */
    public static UIDriver getInstance() {
        if (instance != null) return instance;
        throw new RuntimeException("No UIDriver instance has been created");

    }

    /** Create a new UIDriver instance. Should only every be called once.
     * @param classNames Names of the output classes, in probability distribution passed to addPrediction/s
     * @param normalClassIdx Index of the 'normal' / non-attack index in classNames (and addPredictions INDArray)
     * @param serviceNames Names of the services
     * @param tableConverter TableConverter converts the Collection<Writable> to a table for displaying the details in the UI
     * @param columnsMap a map of column names to integers...
     * @return
     */
    public synchronized static UIDriver createInstance(List<String> classNames, int normalClassIdx, List<String> serviceNames,
                                                       TableConverter tableConverter, Map<String,Integer> columnsMap){
        if(instance != null) throw new RuntimeException("Can only create one UIDriver instance at a time");
        //avoid race conditions on creation

        instance = new UIDriver(classNames,normalClassIdx,serviceNames,tableConverter,columnsMap);
        return instance;
    }


    /** Method for adding a single prediction
     * @param prediction Tuple3 containing: Example number, INDArray of predictions (classification probability distribution), original writables for display
     */
    public void addPrediction(Tuple3<Long,INDArray,List<Writable>> prediction){
        this.predictions.add(prediction);
    }

    /** Mthod for adding a multiple predictions
     * @param predictions Tuple3s containing: Example number, INDArray of predictions (classification probability distribution), original writables for display
     */
    public void addPredictions(List<Tuple3<Long,INDArray,List<Writable>>> predictions){
        this.predictions.addAll(predictions);
    }

    public void shutdown(){
        shutdown.set(true);
    }


    /** UIThreadRunnable: this is where all the processing happens, in an async manner
     *
     */
    private class UIThreadRunnable implements Runnable {

        private long lastUpdateTime = 0;
        private LinkedList<Pair<Long,Double>> connectionRateHistory = new LinkedList<>();
        private LinkedList<Pair<Long,Double>> byteRateHistory = new LinkedList<>();
        private LinkedList<Tuple3<Long,INDArray,List<Writable>>> lastAttacks = new LinkedList<>();

        //Keep a small history here, to smooth out the instantaneous rate calculations (i.e., delta(connections_now - connections_t-3)) etc
        private LinkedList<Long> flowCountHistory = new LinkedList<>();
        private LinkedList<Long> updateTimeHistory = new LinkedList<>();
        private LinkedList<Double> sumBytesHistory = new LinkedList<>();
        private Map<String,LinkedList<Double>> serviceNamesHistory = new HashMap<>();

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

            //Initialize service names history...
            for(String s : serviceNames){
                LinkedList<Double> list = new LinkedList<>();
                list.add(1.0 / serviceNames.size());
                serviceNamesHistory.put(s,list);
            }

            List<Tuple3<Long,INDArray,List<Writable>>> list = new ArrayList<>(100);
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
                int serviceCol = (columnsMap.containsKey("service") ? columnsMap.get("service") : -1);

                Map<String,Integer> serviceCounts = new HashMap<>();
                List<IntRenderElements> renderElementsList = new ArrayList<>();

                for(Tuple3<Long,INDArray,List<Writable>> t3 : list){
                    List<Writable> c = t3._3();
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
                    if(probs[normalClassIdx] < 0.5f){
                        //Attack
                        lastAttacks.add(t3);

                        //This appears to be a bottleneck at times... maybe ok if just attacks though...
                        RenderableComponent rc = tableConverter.rawDataToTable(c);
                        RenderableComponent barChart = new RenderableComponentHorizontalBarChart.Builder()
                                .addValues(classNames,probs)
                                .title("Network Predictions: Attack Type Probabilities")
                                .margins(40,20,150,20)
                                .xMin(0.0).xMax(1.0)
                                .build();


                        RenderElements re = new RenderElements(rc,barChart);
                        // Passing in single attack example which causes chart load issues
//                        WebTarget wt = client.target("http://localhost:8080/flow/update/" + idx);
//                        wt.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                                .post(Entity.entity(re,MediaType.APPLICATION_JSON));
//                        renderElementsList.add(new Pair<>(idx,re));
                        renderElementsList.add(new IntRenderElements(idx,re));
                    }
                }

                // Passing in grouped attack examples in a list which causes chart load issues
                WebTarget wt = client.target("http://localhost:8080/flow/update/");
                wt.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(renderElementsList,MediaType.APPLICATION_JSON));


                if(lastUpdateTime > 0){
                    long pastUpdateTime = updateTimeHistory.getFirst();
                    long pastCount = flowCountHistory.getFirst();
                    long newCount = flowCountHistory.getLast() + list.size();
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
                        .setRemoveAxisHorizontal(true)
                        .legend(false)
                        .margins(30,20,60,20)
                        .addSeries("Connections/sec",time,rate).build();

                connectionRateChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(connectionRate,MediaType.APPLICATION_JSON));

                RenderableComponent byteRate = new RenderableComponentLineChart.Builder()
                        .setRemoveAxisHorizontal(true)
                        .legend(false)
                        .margins(30,20,60,20)
                        .addSeries("kBytes/sec",bytesTime,bytesRate).build();

                bytesRateChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(byteRate,MediaType.APPLICATION_JSON));



                //Now, post details of the last 20 attacks
                //For now: just post details of last 20 FLOWS, whether attacks or not
                while(lastAttacks.size() > NUM_ATTACKS_TO_KEEP) lastAttacks.removeFirst();

                String[][] table = new String[lastAttacks.size()][5];
                int j=0;
                for(Tuple3<Long,INDArray,List<Writable>> t3 : lastAttacks ){
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
                        .paddingPx(5,5,0,0)
                        .border(1)
                        .backgroundColor("#FFFFFF")
                        .headerColor("#CCCCCC")
                        .colWidthsPercent(8,28,28,16,20)
                        .build();

                tableTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(rct,MediaType.APPLICATION_JSON));

                //Calculate new proportions:
                RenderableComponentStackedAreaChart.Builder rcArea = new RenderableComponentStackedAreaChart.Builder()
                        .title("title")
                        .setRemoveAxisHorizontal(true)
                        .margins(30,20,60,20)
                        .setXValues(time);

                double alpha = 0.9;
                double sum = 0.0;
                double[] props = new double[serviceNames.size()];
                int k=0;
                for(String s : serviceNames){
                    LinkedList<Double> history = serviceNamesHistory.get(s);
                    double lastProp;
                    if(history == null){
                        history = new LinkedList<>();
                        serviceNamesHistory.put(s,history);
                        lastProp = 0.0;
                    } else if(history.size() == 0) {
                        lastProp = 0.0;
                    } else {
                        lastProp = history.getLast();
                    }

                    int count = (serviceCounts.containsKey(s) ? serviceCounts.get(s) : 0);
                    double rawProportion = ((double)count)/list.size();

                    double newProp = alpha * lastProp + (1.0-alpha)*rawProportion;
                    props[k++] = newProp;
                    sum += newProp;

                    while(history.size() > time.length) history.removeFirst();
                }
                for(k=0; k<props.length; k++ ){
                    props[k] /= sum;
                }

                k=0;
                for(String s : serviceNames){
                    LinkedList<Double> history = serviceNamesHistory.get(s);
                    history.addLast(props[k++]);

                    while(history.size() > time.length) history.removeFirst();

                    double[] out = new double[time.length];
                    for( int l=0; l<out.length; l++ ){
                        out[l] = history.get(l);
                    }
                    rcArea.addSeries(s,out);
                }



                areaChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(Entity.entity(rcArea.build(),MediaType.APPLICATION_JSON));


                //Clear the list for the next iteration
                list.clear();
            }

            log.info("UI driver thread shutting down");
        }
    }

}