package org.deeplearning4j.examples.ui;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.math3.util.Pair;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.components.*;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.client.Entity;
//import javax.ws.rs.client.WebResource;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**Derived from UIDriver, minus all the dropwizard stuff...
 */
public class UIProcessor {

    public static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory());

    public static final double CHART_HISTORY_SECONDS = 20.0;   //N seconds of chart history for UI
    private static final int NUM_ATTACKS_TO_KEEP = 22;

    private static volatile UIProcessor instance;

    private static final Logger log = LoggerFactory.getLogger(UIProcessor.class);

    private TableConverter tableConverter;
    private Map<String,Integer> columnsMap;
    private List<String> classNames;
    private List<String> serviceNames;
    private int normalClassIdx;

    private LinkedBlockingQueue<Tuple3<Long,INDArray,List<Writable>>> predictions = new LinkedBlockingQueue<>();
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    //Web targets: for posting results
//    private final Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
//    private final WebResource connectionRateChartTarget = client.target("http://localhost:8080/charts/update/connection");
//    private final WebResource bytesRateChartTarget = client.target("http://localhost:8080/charts/update/bytes");
//    private final WebResource tableTarget = client.target("http://localhost:8080/table/update");
//    private final WebResource areaChartTarget = client.target("http://localhost:8080/areachart/update");
    
    private final Client client = Client.create();  //ClientBuilder.newClient().register(JacksonJsonProvider.class);
    private WebResource connectionRateChartTarget;// = client.resource("http://localhost:8080/charts/update/connection");
    private WebResource bytesRateChartTarget;// = client.target("http://localhost:8080/charts/update/bytes");
    private WebResource tableTarget;// = client.target("http://localhost:8080/table/update");
    private WebResource areaChartTarget;// = client.target("http://localhost:8080/areachart/update");
    private Thread uiThread;

    private final String location;

    /**
     * @param location          Location of the dropwizard UI server For example: "http://localhost:8080/"
     */
    private UIProcessor(String location, List<String> classNames, int normalClassIdx, List<String> serviceNames,
                        TableConverter tableConverter, Map<String,Integer> columnsMap) {
        this.location = location;

        connectionRateChartTarget = client.resource(location + "charts/update/connection");
        bytesRateChartTarget = client.resource(location + "charts/update/bytes");
        tableTarget = client.resource(location + "table/update");
        areaChartTarget = client.resource(location + "areachart/update");

        this.classNames = classNames;
        this.normalClassIdx = normalClassIdx;
        this.serviceNames = serviceNames;
        this.tableConverter = tableConverter;
        this.columnsMap = columnsMap;

        //Create UI thread
        uiThread = new Thread(new UIThreadRunnable());
        uiThread.setDaemon(true);
        uiThread.start();

        log.info("UIProcessor started: Location is " + location);
    }

    /** Get an existing UIProcessor instance. If no instance has been manually created, this will throw an exception */
    public static UIProcessor getInstance() {
        if (instance != null) return instance;
        throw new RuntimeException("No UIProcessor instance has been created");

    }

    /** Create a new UIProcessor instance. Should only every be called once.
     * @param location          Location of the dropwizard UI server For example: "http://localhost:8080/"
     * @param classNames Names of the output classes, in probability distribution passed to addPrediction/s
     * @param normalClassIdx Index of the 'normal' / non-attack index in classNames (and addPredictions INDArray)
     * @param serviceNames Names of the services
     * @param tableConverter TableConverter converts the Collection<Writable> to a table for displaying the details in the UI
     * @param columnsMap a map of column names to integers...
     * @return
     */
    public synchronized static UIProcessor createInstance(String location, List<String> classNames, int normalClassIdx, List<String> serviceNames,
                                                          TableConverter tableConverter, Map<String,Integer> columnsMap){
        if(instance != null) throw new RuntimeException("Can only create one UIProcessor instance at a time");
        //avoid race conditions on creation

        instance = new UIProcessor(location, classNames,normalClassIdx,serviceNames,tableConverter,columnsMap);
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
            log.info("Starting UIProcessor thread");

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
                    log.warn("Interrupted exception thrown in UIProcessor thread");
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
                        renderElementsList.add(new IntRenderElements(idx,re));
                    }
                }

                // Passing in grouped attack examples in a list which causes chart load issues
                String renderElementsStr = null;
                try{
                    renderElementsStr = objectMapper.writeValueAsString(renderElementsList);
                }catch(Exception e){
                    e.printStackTrace();
                }
                client.resource(location + "flow/update/")
                        .type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                        .post(ClientResponse.class, renderElementsStr);
//                WebTarget wt = client.target("http://localhost:8080/flow/update/");
//                wt.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                        .post(Entity.entity(renderElementsList,MediaType.APPLICATION_JSON));


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

//                connectionRateChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                        .post(Entity.entity(connectionRate,MediaType.APPLICATION_JSON));

                String connectionRateString = null;
                try{
                    connectionRateString = objectMapper.writeValueAsString(connectionRate);
                }catch(Exception e){
                    e.printStackTrace();
                }

                ClientResponse cs = connectionRateChartTarget.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).post(ClientResponse.class, connectionRateString);

                RenderableComponent byteRate = new RenderableComponentLineChart.Builder()
                        .setRemoveAxisHorizontal(true)
                        .legend(false)
                        .margins(30,20,60,20)
                        .addSeries("kBytes/sec",bytesTime,bytesRate).build();

                String byteRateString = null;
                try{
                    byteRateString = objectMapper.writeValueAsString(byteRate);
                }catch(Exception e){
                    e.printStackTrace();
                }

                cs = bytesRateChartTarget.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).post(ClientResponse.class, byteRateString);
//                bytesRateChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                        .post(Entity.entity(byteRate,MediaType.APPLICATION_JSON));



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

//                tableTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                        .post(Entity.entity(rct,MediaType.APPLICATION_JSON));

                String rctString = null;
                try{
                    rctString = objectMapper.writeValueAsString(rct);
                }catch(Exception e){
                    e.printStackTrace();
                }

                cs = tableTarget.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).post(ClientResponse.class, rctString);

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



//                areaChartTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                        .post(Entity.entity(rcArea.build(),MediaType.APPLICATION_JSON));
                String areaString = null;
                try{
                    areaString = objectMapper.writeValueAsString(rcArea.build());
                }catch(Exception e){
                    e.printStackTrace();
                }

                areaChartTarget.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).post(ClientResponse.class, areaString);


                //Clear the list for the next iteration
                list.clear();
            }

            log.info("UIProcessor thread shutting down");
        }
    }

}