package com.plarium.south.superflow.core.spec.sink.bigquery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.sink.mapping.BQSinkMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class BigQuerySinkTest {

    private String sinkName = "sink-name";
    private String tupleTag = "bq-output-dataset";
    private String allowCreateNew = "true";
    private String writeMode = BQWriteMode.append.toString();
    private String tableId = "project.dataset.table";

    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("sink.name", sinkName);
        env.put("tuple.tag", tupleTag);
        env.put("allow.create.table", allowCreateNew);
        env.put("write.mode", writeMode);
        env.put("table.id", tableId);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sinks/bigquery/bigquery-sink.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<BigQuerySink> type = new TypeReference<BigQuerySink>() {};
        List<BigQuerySink> sinks = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (BigQuerySink sink : sinks) {
            sink.setup();

            assertEquals(sink.getName(), sinkName);
            MappingList<BQSinkMapping> mapping = sink.getMapping();
            assertNotNull(mapping);
            assertNotNull(mapping.getList());
            assertFalse(mapping.getList().isEmpty());

            BQSinkMapping map1 = mapping.getList().get(0);
            assertEquals(map1.getTag(), tupleTag);
            assertEquals(map1.getTable(), tableId);
            assertEquals(map1.getWriteMode(), BQWriteMode.valueOf(writeMode));
            assertEquals(map1.getAllowCreate(), Boolean.parseBoolean(allowCreateNew));
        }
    }

}