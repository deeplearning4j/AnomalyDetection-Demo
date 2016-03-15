package org.deeplearning4j.examples.ui.resources;

import org.deeplearning4j.examples.ui.components.RenderElements;
import org.deeplearning4j.examples.ui.components.RenderableComponentString;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Alex on 14/03/2016.
 */
@Path("/flow")
@Produces(MediaType.APPLICATION_JSON)
public class FlowDetailsResource {

    private Map<Integer,RenderElements> map = new ConcurrentHashMap<>();

    private static final RenderElements NOT_FOUND = new RenderElements(new RenderableComponentString("(Not found)"));

    @GET
    @Path("/{id}")
    public Response getFlowDetails(@PathParam("id") int flowNumber){
        if(!map.containsKey(flowNumber)) return Response.ok(NOT_FOUND).build();
        return Response.ok(map.get(flowNumber)).build();
    }

    @POST
    @Path("/update/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response update(@PathParam("id")int flowNumber, RenderElements renderElements){
        map.put(flowNumber,renderElements);
        return Response.ok(Collections.singletonMap("status", "ok")).build();
    }
}