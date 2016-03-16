package org.deeplearning4j.examples.ui.resources;

import org.deeplearning4j.examples.ui.components.RenderableComponent;
import org.deeplearning4j.examples.ui.components.RenderableComponentLineChart;
import org.deeplearning4j.examples.ui.components.RenderableComponentStackedAreaChart;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * Created by Alex on 14/03/2016.
 */
@Path("/areachart")
@Produces(MediaType.APPLICATION_JSON)
public class AreaChartResource {

    private RenderableComponent chart;

    @GET
    public Response getChart(){
        return Response.ok(chart).build();
    }

    @POST
    @Path("/update/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response update(RenderableComponentStackedAreaChart chart){
        this.chart = chart;
        return Response.ok(Collections.singletonMap("status", "ok")).build();
    }
}
