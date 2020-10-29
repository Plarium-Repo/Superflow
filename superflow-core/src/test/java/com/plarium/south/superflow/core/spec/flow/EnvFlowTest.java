package com.plarium.south.superflow.core.spec.flow;

import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class EnvFlowTest {

    @Test
    public void testSpecYamlParsing() throws IOException {
        String spec =
                        "---\n" +
                        "type: flow/env\n" +
                        "env:\n" +
                        "  var1: v1\n" +
                        "  var2: v2\n" +
                        "  var3: v3\n";

        EnvFlow env = ParseUtils.parseYaml(spec.getBytes(), EnvFlow.class);
        assertEquals(env.getEnv().get("var1"), "v1");
        assertEquals(env.getEnv().get("var2"), "v2");
        assertEquals(env.getEnv().get("var3"), "v3");
    }
}