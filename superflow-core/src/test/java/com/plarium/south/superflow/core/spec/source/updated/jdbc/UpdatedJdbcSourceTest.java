package com.plarium.south.superflow.core.spec.source.updated.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.source.mapping.UpdatedJdbcSourceMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class UpdatedJdbcSourceTest {

    private String sourceName = "jdbc-updated-source";
    private final String jdbcUrl = "jdbc:postgresql://localhost/test";
    private final String jdbcUsername = "username";
    private final String jdbcPassword = "password";
    private final String jdbcDriverJars = "/tmp/jdbc/jar1.jar/tmp/jdbc/jar2.jar";
    private final String jdbcDriverClass = "org.postgresql.Driver";
    private final String jdbcUrlProperties = "ssl=true;protocolVersion=V3";
    private final String eventTime = "ts";
    private final String duration = "1 day";
    private final String tupleTag = "tag";
    private final String query = "select 1";


    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("source.name", sourceName);
        env.put("jdbc.url", jdbcUrl);
        env.put("jdbc.username", jdbcUsername);
        env.put("jdbc.password", jdbcPassword);
        env.put("jdbc.driver.jars", jdbcDriverJars);
        env.put("jdbc.driver.class", jdbcDriverClass);
        env.put("jdbc.url.properties", jdbcUrlProperties);
        env.put("event.time.field", eventTime);
        env.put("tuple.tag", tupleTag);
        env.put("time.between.data.update", duration);
        env.put("input.jdbc.query", query);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sources/updated/jdbc/jdbc-updated-source.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<UpdatedJdbcSource> type = new TypeReference<UpdatedJdbcSource>() {};
        List<UpdatedJdbcSource> sources = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (UpdatedJdbcSource source : sources) {
            source.setup();
            assertEquals(source.getName(), sourceName);

            DataSourceConfig dataSource = source.getDataSource();
            assertNotNull(dataSource);
            assertEquals(dataSource.getUrl(), jdbcUrl);
            assertEquals(dataSource.getUsername(), jdbcUsername);
            assertEquals(dataSource.getPassword(), jdbcPassword);
            assertEquals(dataSource.getDriverJars(), jdbcDriverJars);
            assertEquals(dataSource.getDriverClass(), jdbcDriverClass);
            assertEquals(dataSource.getUrlProperties(), jdbcUrlProperties);

            UpdatedJdbcSourceMappingList mapping = source.getMapping();
            assertNotNull(mapping);
            assertEquals(mapping.getDuration(), duration);
            assertEquals(mapping.getEventTime(), eventTime);

            assertNotNull(mapping.getList());
            assertEquals(1, mapping.getList().size());

            UpdatedJdbcSourceMapping map1 = mapping.getList().get(0);
            assertEquals(map1.getDuration(), duration);
            assertEquals(map1.getTag(), tupleTag);
            assertEquals(map1.getQuery(), query);
        }
    }
}