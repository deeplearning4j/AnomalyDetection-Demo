package org.deeplearning4j.examples.ui2.temp;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class JerseyMapperProvider implements ContextResolver<ObjectMapper> {
    private static ObjectMapper apiMapper = new ObjectMapper();
    @Override
    public ObjectMapper getContext(Class<?> type)
    {
        return apiMapper;
    }
}
