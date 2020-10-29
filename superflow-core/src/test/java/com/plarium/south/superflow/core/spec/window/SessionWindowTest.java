package com.plarium.south.superflow.core.spec.window;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.utils.ParseUtils;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SessionWindowTest extends TestSpecUtils {

    private String gapSize = "10 min";
    private String allowedLateness = "10 min";
    private String timeCombiner = "earliest";
    private String accumulateWindow = "false";


    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("window.gap.size", gapSize);
        env.put("allowed.lateness", allowedLateness);
        env.put("window.time.combiner", timeCombiner);
        env.put("accumulate.windows", accumulateWindow);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/windows/session-windows.yaml";
        String yamlTemplated = renderTemplate(path, getEnv());
        TypeReference<SessionWindow> type = new TypeReference<SessionWindow>() {};
        List<SessionWindow> windows = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (SessionWindow window : windows) {
            assertEquals(window.getGapSize(), gapSize);
            assertEquals(window.getAllowedLateness(), allowedLateness);
            assertEquals(window.getTimeCombiner(), TimeCombiner.valueOf(timeCombiner));
            assertEquals(window.getAccumulateWindow(), Boolean.valueOf(accumulateWindow));
        }
    }
}