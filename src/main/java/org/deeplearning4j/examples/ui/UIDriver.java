package org.deeplearning4j.examples.ui;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.ui.components.RenderElements;
import org.deeplearning4j.examples.ui.components.RenderableComponent;
import org.deeplearning4j.examples.ui.config.NIDSConfig;
import org.deeplearning4j.examples.ui.resources.FlowDetailsResource;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Alex on 14/03/2016.
 */
public class UIDriver extends Application<NIDSConfig> {

    private static volatile UIDriver instance;

    private static final Logger log = LoggerFactory.getLogger(UIDriver.class);

    private static TableConverter tableConverter;

    private LinkedBlockingQueue<Tuple3<Long,INDArray,Collection<Writable>>> predictions = new LinkedBlockingQueue<>();
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    //Web targets: for posting results
    private final Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
//    private final WebTarget targetFlowResourceUpdate = client.target("http://localhost:8080/flow/update")

    private Thread uiThread;

    private UIDriver(){
        super();

        try{
            run("server", "dropwizard.yml");
        }catch(Exception e){
            throw new RuntimeException(e);
        }

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
//        environment.jersey().register(new SummaryStatusResource());
//        environment.jersey().register(new ConfigResource());
//        environment.jersey().register(new SummaryResultsResource());
//        environment.jersey().register(new CandidateResultsResource());
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


    public void addPrediction(Tuple3<Long,INDArray,Collection<Writable>> prediction){
        this.predictions.add(prediction);
    }

    public void addPredictions(List<Tuple3<Long,INDArray,Collection<Writable>>> predictions){
        this.predictions.addAll(predictions);
    }

    public void shutdown(){
        shutdown.set(true);
    }


    private class UIThreadRunnable implements Runnable {
        @Override
        public void run() {
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
                System.out.println("-----------------------------------------------");
                for(Tuple3<Long,INDArray,Collection<Writable>> t3 : list){
                    System.out.println(t3);

                    //Post the details to the web server:
                    int idx = (int)((long)t3._1());
                    Collection<Writable> c = t3._3();

                    RenderableComponent rc = tableConverter.rawDataToTable(c);
                    RenderElements re = new RenderElements(rc);
                    WebTarget wt = client.target("http://localhost:8080/flow/update/" + idx);
                    wt.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                            .post(Entity.entity(re,MediaType.APPLICATION_JSON));
                }

                System.out.println("///////////////////////////////////////////////");

                //Clear the list for the next iteration
                list.clear();
            }

            log.info("UI driver thread shutting down");
        }
    }

}
