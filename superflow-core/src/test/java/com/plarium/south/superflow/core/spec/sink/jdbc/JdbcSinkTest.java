package com.plarium.south.superflow.core.spec.sink.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.commons.jdbc.StatementMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JdbcSinkTest {

    private final String sinkName = "sink-name";
    private final String inputTag = "input-dataset-name";
    private final String batchSize = "100";
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
        env.put("sink.name", sinkName);
        env.put("input.dataset.name", inputTag);
        env.put("write.batch.size", batchSize);
        env.put("sql.template", sqlTemplate);
        env.put("sql.var.order", sqlVarOrder);
        env.put("sql.var.type", sqlVarType.toString().toLowerCase());
        env.put("sql.var.name", sqlVarName);
        env.put("jdbc.url", jdbcUrl);
        env.put("jdbc.username", jdbcUsername);
        env.put("jdbc.password", jdbcPassword);
        env.put("jdbc.driver.jars", jdbcDriverJars);
        env.put("jdbc.driver.class", jdbcDriverClass);
        env.put("jdbc.properties", jdbcUrlProperties);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sinks/jdbc/jdbc-sink.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<JdbcSink> type = new TypeReference<JdbcSink>() {};
        List<JdbcSink> sinks = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (JdbcSink sink : sinks) {
            sink.setup();

            assertEquals(sink.getType(), JdbcSink.TYPE);
            assertEquals(sink.getName(), sinkName);
            assertEquals(sink.getInputTag(), inputTag);
            assertEquals(sink.getSqlTemplate(), sqlTemplate);
            assertEquals(sink.getBatchSize(), Long.valueOf(batchSize));


            DataSourceConfig dataSource = sink.getDataSource();
            assertEquals(dataSource.getUrl(), jdbcUrl);
            assertEquals(dataSource.getUsername(), jdbcUsername);
            assertEquals(dataSource.getPassword(), jdbcPassword);
            assertEquals(dataSource.getDriverJars(), jdbcDriverJars);
            assertEquals(dataSource.getDriverClass(), jdbcDriverClass);
            assertEquals(dataSource.getUrlProperties(), jdbcUrlProperties);

            List<StatementMapping> variables = sink.getVariables();
            StatementMapping mapping = variables.get(0);
            assertEquals(mapping.getOrder(), Integer.valueOf(sqlVarOrder));
            assertEquals(mapping.getName(), sqlVarName);
            assertEquals(mapping.getType(), sqlVarType);
        }
    }
}