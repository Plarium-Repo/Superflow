package com.plarium.south.superflow.core.spec;

import com.plarium.south.superflow.core.utils.TemplateUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class TestSpecUtils {

    public static String renderTemplate(String path, Map<String, String> env) throws IOException {
        return TemplateUtils.renderYamlSpec(getResourceAsUTF8String(path), env);
    }

    public static String getResourceAsUTF8String(String path) throws IOException {
        return IOUtils.toString(TestSpecUtils.class.getResourceAsStream(path), StandardCharsets.UTF_8);
    }
}
