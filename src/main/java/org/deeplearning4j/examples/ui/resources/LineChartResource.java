package org.deeplearning4j.examples.ui.resources;

import org.deeplearning4j.examples.ui.components.RenderElements;
import org.deeplearning4j.examples.ui.components.RenderableComponent;
import org.deeplearning4j.examples.ui.components.RenderableComponentLineChart;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * Created by Alex on 14/03/2016.
 */
@Path("/charts")
@Produces(MediaType.APPLICATION_JSON)
public class LineChartResource {

    private RenderableComponent unknown = null; //new RenderableComponentLineChart.Builder().build();
    private RenderableComponent connectionRateChart = null; //new RenderableComponentLineChart.Builder().build();
    private RenderableComponent bytesRateChart = null;  //new RenderableComponentLineChart.Builder().build();

    @GET
    @Path("/{chart}")
    public Response getChart(@PathParam("chart") String chartName){
        RenderableComponent chart;
        switch (chartName){
            case "connection":
                chart = connectionRateChart;
                break;
            case "bytes":
                chart = bytesRateChart;
                break;
            default:
                chart = unknown;
        }
        return Response.ok(chart).build();
    }

    @POST
    @Path("/update/{chart}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response update(@PathParam("chart")String chartName, RenderableComponentLineChart chart){
        switch (chartName){
            case "connection":
                connectionRateChart = chart;
                break;
            case "bytes":
                bytesRateChart = chart;
                break;
        }
        return Response.ok(Collections.singletonMap("status", "ok")).build();
    }
}
