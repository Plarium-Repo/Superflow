package com.plarium.south.superflow.core.spec.window;

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

public class SlidingWindowTest extends TestSpecUtils {

    private String windowSize = "1 hour";
    private String newEvery = "30 min";
    private String allowedLateness = "10 min";
    private String timeCombiner = "end_of_window";
    private String accumulateWindow = "true";


    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("window.size", windowSize);
        env.put("window.new.every", newEvery);
        env.put("allowed.lateness", allowedLateness);
        env.put("window.time.combiner", timeCombiner);
        env.put("accumulate.windows", accumulateWindow);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/windows/sliding-windows.yaml";
        String yamlTemplated = renderTemplate(path, getEnv());
        TypeReference<SlidingWindow> type = new TypeReference<SlidingWindow>() {};
        List<SlidingWindow> windows = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (SlidingWindow window : windows) {
            assertEquals(window.getSize(), windowSize);
            assertEquals(window.getNewEvery(), newEvery);
            assertEquals(window.getAllowedLateness(), allowedLateness);
            assertEquals(window.getTimeCombiner(), TimeCombiner.valueOf(timeCombiner));
            assertEquals(window.accumulateWindow, Boolean.valueOf(accumulateWindow));
        }
    }
}