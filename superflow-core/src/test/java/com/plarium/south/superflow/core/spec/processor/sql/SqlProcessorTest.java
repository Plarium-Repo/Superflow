package com.plarium.south.superflow.core.spec.processor.sql;

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

public class SqlProcessorTest {

    private final String processorName = "proc-name";
    private final String outputTag = "output-dataset-name";
    private final String sqlTemplate = "select * from COLLECTION";

    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("processor.name", processorName);
        env.put("sql.template", sqlTemplate);
        env.put("output.dataset.name", outputTag);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/processor/sql/sql-processor.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<SqlProcessor> type = new TypeReference<SqlProcessor>() {};
        List<SqlProcessor> processors = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (SqlProcessor processor : processors) {
            processor.setup();

            assertEquals(processor.getType(), SqlProcessor.TYPE);
            assertEquals(processor.getName(), processorName);
            assertEquals(processor.getOutputTag(), outputTag);
            assertEquals(processor.getSqlExpression(), sqlTemplate);
        }
    }
}