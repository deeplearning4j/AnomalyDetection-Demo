package org.deeplearning4j.examples.ui.resources;

import org.deeplearning4j.examples.ui.components.RenderableComponent;
import org.deeplearning4j.examples.ui.components.RenderableComponentLineChart;
import org.deeplearning4j.examples.ui.components.RenderableComponentTable;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * Created by Alex on 15/03/2016.
 */
@Path("/table")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

    private RenderableComponent table = new RenderableComponentTable(new String[]{"#","Source","Destination","Type"},
            new String[0][4]);

    @GET
    public Response getTable(){
        return Response.ok(table).build();
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response update(RenderableComponentTable table){
        this.table = table;
        return Response.ok(Collections.singletonMap("status", "ok")).build();
    }

}
