package com.plarium.south.superflow.core.spec.processor.splitter.string;

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

public class StringSplitterProcessorTest {

    private final String processorName = "proc-name";
    private final String inputFieldName = "inputFieldName";
    private final String outputFieldName = "outputFieldName";
    private final String stringSplitter = "','";
    private final String dropInputField = "true";
    private final String castElement = "int";
    private final String inputTag = "input-tag";
    private final String outputTag = "output-tag";

    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("processor.name", processorName);
        env.put("input.field.name", inputFieldName);
        env.put("output.field.name", outputFieldName);
        env.put("string.splitter", stringSplitter);
        env.put("drop.input.field", dropInputField);
        env.put("cast.element.to", castElement);
        env.put("input.tag", inputTag);
        env.put("output.tag", outputTag);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/processor/splitter/string-splitter.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<StringSplitterProcessor> type = new TypeReference<StringSplitterProcessor>() {};
        List<StringSplitterProcessor> processors = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (StringSplitterProcessor processor : processors) {
            processor.setup();

            assertEquals(processor.getType(), StringSplitterProcessor.TYPE);
            assertEquals(processor.getName(), processorName);
            assertEquals(processor.getInputField(), inputFieldName);
            assertEquals(processor.getOutputField(), outputFieldName);
            assertEquals(processor.getSplitter(), stringSplitter.replace("'", ""));
            assertEquals(processor.getDropInput(), Boolean.valueOf(dropInputField));
            assertEquals(processor.getCastElement(), CastVariableType.valueOf(castElement.toUpperCase()));
            assertEquals(processor.getInputTag(), inputTag);
            assertEquals(processor.getOutputTag(), outputTag);
        }
    }
}