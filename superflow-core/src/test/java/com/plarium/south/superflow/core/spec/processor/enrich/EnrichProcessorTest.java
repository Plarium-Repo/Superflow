package com.plarium.south.superflow.core.spec.processor.enrich;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.processor.enrich.jdbc.JdbcEnricher;
import com.plarium.south.superflow.core.spec.commons.jdbc.StatementMapping;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.registry.HortonRegistry;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class EnrichProcessorTest {

    private final String processorName = "proc-name";
    private final String registryUrl = "http://endpoint/api/v1";
    private final String inputTag = "input-dataset-name";
    private final String outputTag = "output-dataset-name";
    private final String sqlTemplate = "select * from db.table where f1 = ?";
    private final String sqlVarOrder = "1";
    private final StatementMapping.VariableType sqlVarType = StatementMapping.VariableType.STRING;
    private final String sqlVarName = "varName";
    private final String jdbcUrl = "jdbc:postgresql://localhost/test";
    private final String jdbcUsername = "username";
    private final String jdbcPassword = "password";
    private final String jdbcDriverJars = "/tmp/jdbc/jar1.jar/tmp/jdbc/jar2.jar";
    private final String jdbcDriverClass = "org.postgresql.Driver";
    private final String jdbcUrlProperties = "ssl=true;protocolVersion=V3";

    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("processor.name", processorName);
        env.put("registry.url", registryUrl);
        env.put("input.dataset.name", inputTag);
        env.put("output.dataset.name", outputTag);
        env.put("sql.template", sqlTemplate);
        env.put("sql.var.order", sqlVarOrder);
        env.put("sql.var.type", sqlVarType.toString().toLowerCase());
        env.put("sql.var.name", sqlVarName);
        env.put("jdbc.url", jdbcUrl);
        env.put("jdbc.username", jdbcUsername);
        env.put("jdbc.password", jdbcPassword);
        env.put("jdbc.driver.jars", jdbcDriverJars);
        env.put("jdbc.driver.class", jdbcDriverClass);
        env.put("jdbc.url.properties", jdbcUrlProperties);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String yamlTemplated = TestSpecUtils.renderTemplate("/templates/processor/enrich/enrich-processor.yaml", getEnv());
        TypeReference<EnrichProcessor> type = new TypeReference<EnrichProcessor>() {};
        List<EnrichProcessor> processors = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (EnrichProcessor processor : processors) {
            processor.setup();

            assertEquals(processor.getType(), EnrichProcessor.TYPE);
            assertEquals(processor.getName(), processorName);
            assertEquals(processor.getInputTag(), inputTag);
            assertEquals(processor.getOutputTag(), outputTag);

            BaseRegistry registry = processor.getRegistry();
            assertEquals(registry.getType(), HortonRegistry.TYPE);
            assertEquals(registry.getUrl(), registryUrl);

            JdbcEnricher enricher = (JdbcEnricher) processor.getEnricher();
            assertEquals(enricher.getSqlTemplate(), sqlTemplate);

            StatementMapping var1 = enricher.getVariables().get(0);
            assertEquals(var1.getOrder(), Integer.valueOf(sqlVarOrder));
            assertEquals(var1.getType(), sqlVarType);
            assertEquals(var1.getName(), sqlVarName);

            DataSourceConfig source = enricher.getDataSource();
            assertEquals(source.getUrl(), jdbcUrl);
            assertEquals(source.getUsername(), jdbcUsername);
            assertEquals(source.getPassword(), jdbcPassword);
            assertEquals(source.getDriverJars(), jdbcDriverJars);
            assertEquals(source.getDriverClass(), jdbcDriverClass);
            assertEquals(source.getUrlProperties(), jdbcUrlProperties);
        }
    }
}