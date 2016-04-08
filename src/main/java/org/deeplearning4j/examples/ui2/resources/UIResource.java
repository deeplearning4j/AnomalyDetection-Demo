package org.deeplearning4j.examples.ui2.resources;

import io.dropwizard.views.View;
import org.deeplearning4j.examples.ui.resources.UIView;
import org.deeplearning4j.ui.api.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alex on 14/03/2016.
 */
@Path("/ui")
@Produces(MediaType.TEXT_HTML)
public class UIResource {

    private long lastUpdateTime = 0L;
//    private Component[] components;
    private List<Component> components = new ArrayList<>();

    @GET
    public View get(){
        return new UI2View();
    }

    @GET
    @Path("/lastUpdateTime")
    public Response getLastUpdateTime(){
        return Response.ok(lastUpdateTime).build();
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
//    public Response update(Component[] components){
    public Response update(List<Component> components){
        this.components = components;
        lastUpdateTime = System.currentTimeMillis();
        System.out.println("*** GOT COMPONENTS: " + this.components);
        return Response.ok(Collections.singletonMap("status", "ok")).build();
    }

    @GET
    @Path("components")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getComponents(){
        return Response.ok(components).build();
    }

}
