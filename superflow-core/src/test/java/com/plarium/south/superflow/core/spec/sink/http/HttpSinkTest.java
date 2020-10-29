package com.plarium.south.superflow.core.spec.sink.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class HttpSinkTest {

    private String sinkName = "sink-name";
    private String httpMethod = "POST";
    private String contentType = "application/json";
    private String numberOfRetries = "3";
    private String httpEndpoint = "host:port";
    private String tupleTag = "http-output";
    private String bodyFilter = "$.gameId";
    private String eventTime = "ts";


    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("sink.name", sinkName);
        env.put("http.method", httpMethod);
        env.put("content.type", contentType);
        env.put("number.of.retries", numberOfRetries);
        env.put("http.endpoint", httpEndpoint);
        env.put("tuple.tag", tupleTag);
        env.put("body.filter", bodyFilter);
        env.put("event.time", eventTime);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sinks/http/http-sink.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<HttpSink> type = new TypeReference<HttpSink>(){};
        List<HttpSink> sinks = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (HttpSink sink : sinks) {
            sink.setup();

            assertEquals(sink.getName(), sinkName);
            assertEquals(sink.getMethod(), RequestMethod.valueOf(httpMethod));
            assertEquals(sink.getContentType(), contentType);
            assertNotNull(sink.getHttpConfig());
            assertEquals(sink.getHttpConfig().getNumberOfRetries(), Integer.valueOf(numberOfRetries));
            assertTrue(sink.getUrlTemplate().contains(httpEndpoint));

            assertNotNull(sink.getMapping());
            assertEquals(sink.getMapping().getTag(), tupleTag);
            assertTrue(sink.getBodyFilter().contains(bodyFilter));
            assertEquals(sink.getMapping().getEventTime(), eventTime);
        }
    }
}