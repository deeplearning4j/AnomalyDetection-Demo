package org.deeplearning4j.examples.ui2;

//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
//import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
//import com.google.common.collect.ImmutableMap;
//import io.dropwizard.Application;
//import io.dropwizard.assets.AssetsBundle;
//import io.dropwizard.setup.Bootstrap;
//import io.dropwizard.setup.Environment;
//import io.dropwizard.views.ViewBundle;
//import org.canova.api.writable.Writable;
//import org.deeplearning4j.examples.ui2.resources.UIResource;
//import org.deeplearning4j.ui.api.Component;
//import org.glassfish.jersey.server.ResourceConfig;
//import org.glassfish.jersey.server.model.Resource;
//import org.nd4j.linalg.api.ndarray.INDArray;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import scala.Tuple3;
//
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.client.Entity;
//import javax.ws.rs.client.WebTarget;
//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response;
//import java.util.*;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Alex on 8/04/2016.
 */
public abstract class StreamingUI { // extends Application<UiConfig> {
//
//    private static final Logger log = LoggerFactory.getLogger(StreamingUI.class);
//
//    private static volatile StreamingUI instance;
//
//    private LinkedBlockingQueue<Tuple3<Long, INDArray, List<Writable>>> predictionsToProcess = new LinkedBlockingQueue<>();
//
//    //Web target for posting results:
//    private final Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
//    private final WebTarget uiUpdateTarget = client.target("http://localhost:8080/ui/update");
//
//    //Thread that calls processRecords function periodically:
//    private Thread uiThread;
//    private AtomicBoolean shutdown = new AtomicBoolean(false);
//    private Component[] lastComponents;
//    //Last time that updateUI(Component...) was called
//    private long lastUIComponentUpdateTime = 0L;
//    //Last time that the UI components were posted to the server
//    private long lastUIComponentPostTime = 0L;
//
//    public StreamingUI() {
//
//        //TODO: this is hacky, and needs to be done properly
//        instance = this;
//
//        try {
//            run("server", "dropwizard.yml");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        //Create UI thread
//        uiThread = new Thread(new UIProcessingThreadRunnable());
//        uiThread.setDaemon(true);
//        uiThread.start();
//
//        log.info("UIDriver started: http://localhost:8080/ui/");
//
//    }
//
//    public static StreamingUI getInstance(){
//        return instance;
//    }
//
//    @Override
//    public void run(UiConfig uiConfig, Environment environment) throws Exception {
//        environment.jersey().register(new UIResource());
//    }
//
//    @Override
//    public String getName() {
//        return "StreamingUI";
//    }
//
//    @Override
//    public void initialize(Bootstrap<UiConfig> bootstrap) {
//        //Necessary to avoid abstract method issue with certain dependencies/versions of dropwizard
//        bootstrap.addBundle(new ViewBundle<UiConfig>() {
//            @Override
//            public Map<String, Map<String, String>> getViewConfiguration(UiConfig nidsConfig) {
//                return ImmutableMap.of();
//            }
//        });
//        bootstrap.addBundle(new AssetsBundle());
//    }
//
//    /**
//     * Implement this method for processing new records. This will be run async
//     * This method should periodically call updateUI to update the user interface
//     *
//     * @param newRecords    Most recent records to be processed
//     */
//    public abstract void processRecords(List<Tuple3<Long, INDArray, List<Writable>>> newRecords);
//
//
//    /**
//     * Receive a single prediction (to be processed and/or displayed in the UI)
//     */
//    public void receivePrediction(Tuple3<Long, INDArray, List<Writable>> prediction) {
//        receivePredictions(Collections.singletonList(prediction));
//    }
//
//    /**
//     * Receive a multiple predictions (to be processed and/or displayed in the UI)
//     */
//    public void receivePredictions(List<Tuple3<Long, INDArray, List<Writable>>> predictions) {
//        predictionsToProcess.addAll(predictions);
//    }
//
//
//    /**
//     * Update the UI. This should be called periodically by processRecords
//     *
//     * @param components New components to display in the UI
//     */
//    protected void updateUI(Component... components) {
//        this.lastComponents = components;
//        lastUIComponentUpdateTime = System.currentTimeMillis();
//        System.out.println("***** UPDATEUI WAS CALLED. TIME=" + lastUIComponentUpdateTime + " *****");
//    }
//
//    /**
//     * This runnable is responsible for:
//     * - batching updates: i.e., periodically calling ProcessRecords on the new data
//     * - posting the updates provided by processRecords (via that method calling updateUI)
//     */
//    private class UIProcessingThreadRunnable implements Runnable {
//
//        private static final long updateFrequency = 2000L;  //milliseconds
//
//
//        @Override
//        public void run() {
//            try {
//                runHelper();
//            } catch (Exception e) {
//                log.error("Unchecked exception thrown during UI processing", e);
//                e.printStackTrace();
//            }
//        }
//
//        private void runHelper() {
//
//            System.out.println("***** WE'RE DOING THE PROCESSING THING *****");
//
//            List<Tuple3<Long, INDArray, List<Writable>>> list = new ArrayList<>(100);
//
//            while (!shutdown.get()) {
//
//                try {
//                    list.add(predictionsToProcess.take());   //Blocks if no elements are available
//                } catch (InterruptedException e) {
//                    log.warn("Interrupted exception thrown in UI driver thread");
//                }
//                predictionsToProcess.drainTo(list);      //Doesn't block, but retrieves + removes all elements
//
//                System.out.println("***** GOT SOME DATA *****");
//
//                long processStartTime = System.currentTimeMillis();
//                processRecords(list);
//                list.clear();
//
//
//
//                //Update the UI, if necessary:
//                if (lastUIComponentUpdateTime != lastUIComponentPostTime) {
//                    Response resp = uiUpdateTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
//                            .post(Entity.entity(lastComponents, MediaType.APPLICATION_JSON));
//
//                    if(resp.getStatus() >= 400 && resp.getStatus() < 600) log.warn("UI update response: {}",resp);
//
//                    lastUIComponentPostTime = lastUIComponentUpdateTime;
//
//                    System.out.println("***** POSTED THE THINGS ***** " + Arrays.toString(lastComponents));
//                }
//
//                //Now, wait...
//                long waitTime = updateFrequency - (System.currentTimeMillis() - processStartTime);
//                if (waitTime > 0) {
//                    try {
//                        Thread.sleep(waitTime);
//                    } catch (InterruptedException e) {
//                        log.warn("Interrupted exception thrown by Thread.sleep() for UI thread");
//                    }
//                }
//            }
//        }
//    }
}
