package org.deeplearning4j.examples.ui.resources;

import io.dropwizard.views.View;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by Alex on 14/03/2016.
 */
@Path("/intrusion")
@Produces(MediaType.TEXT_HTML)
public class UIResource {

    @GET
    public View get(){
        return new UIView();
    }

}
