package com.plarium.south.superflow.core.spec.source.bigquery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.source.mapping.BQSourceMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class BigQuerySourceTest {

    private String sourceName = "bq-source";
    private String eventTime = "timestamp";
    private String tupleTag = "dataset-name";
    private String bqQuery = "SELECT * FROM `project.dataset.table`";

    protected Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("source.name", sourceName);
        env.put("event.time.field", eventTime);
        env.put("tuple.tag", tupleTag);
        env.put("input.bq.query", bqQuery);
        return env;
    }


    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sources/bigquery/bigquery-source.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<BigQuerySource> type = new TypeReference<BigQuerySource>() {};
        List<BigQuerySource> sources = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (BigQuerySource source : sources) {
            source.setup();

            assertEquals(source.getName(), sourceName);

            MappingList<BQSourceMapping> mapping = source.getMapping();
            assertNotNull(mapping);

            BQSourceMapping queryMapping = mapping.getList().get(0);
            assertEquals(queryMapping.getEventTime(), eventTime);
            assertEquals(queryMapping.getQuery(), bqQuery);
            assertEquals(queryMapping.getTag(), tupleTag);
        }
    }
}