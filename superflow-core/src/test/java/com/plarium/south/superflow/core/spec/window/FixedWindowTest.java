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

import static org.junit.Assert.assertEquals;

public class FixedWindowTest extends TestSpecUtils {

    private String windowSize = "1 hour";
    private String allowedLateness = "1 min";
    private String timeCombiner = "latest";
    private String accumulateWindow = "false";


    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("window.size", windowSize);
        env.put("allowed.lateness", allowedLateness);
        env.put("window.time.combiner", timeCombiner);
        env.put("accumulate.windows", accumulateWindow);
        return env;
    }
    
    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/windows/fixed-windows.yaml";
        String yamlTemplated = renderTemplate(path, getEnv());
        TypeReference<FixedWindow> type = new TypeReference<FixedWindow>() {};
        List<FixedWindow> windows = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (FixedWindow window : windows) {
            assertEquals(window.getSize(), windowSize);
            assertEquals(window.getAllowedLateness(), allowedLateness);
            assertEquals(window.getTimeCombiner(), TimeCombiner.valueOf(timeCombiner));
            assertEquals(window.getAccumulateWindow(), Boolean.valueOf(accumulateWindow));
        }
    }
}