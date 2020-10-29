package com.plarium.south.superflow.core.spec.sink.fs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.registry.HortonRegistry;
import com.plarium.south.superflow.core.spec.sink.mapping.FsSinkMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class AvroFileStoreSinkTest {

    private String sinkName = "sink-name";
    private String numOfShards = "1";
    private String tempDir = "/tmp/events";
    private String location = "/path/to/data";
    private String registryUrl = "http://endpoint/v1/api";
    private String avroSchemaGroup = "avroSchemaGroup";
    private String avroSchemaName = "avroSchemaName";
    private String datasetName = "dataset-name";
    private String datasetPath = "/dataset/**";
    private String createSchema = "true";
    private String inferSchema = "true";

    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("sink.name", sinkName);
        env.put("sink.num.shards", numOfShards);
        env.put("sink.tmp.dir", tempDir);
        env.put("sink.output", location);
        env.put("registry.url", registryUrl);
        env.put("avro.schema.group", avroSchemaGroup);
        env.put("avro.schema.name", avroSchemaName);
        env.put("tuple.tag", datasetName);
        env.put("dataset.path", datasetPath);
        env.put("create.schema", createSchema);
        env.put("infer.schema", inferSchema);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sinks/fs/avro-fs-sink.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<AvroFileStoreSink> type = new TypeReference<AvroFileStoreSink>() {};
        List<AvroFileStoreSink> sinks = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (AvroFileStoreSink sink : sinks) {
            sink.setup();

            assertEquals(sink.getType(), AvroFileStoreSink.TYPE);
            Assert.assertEquals(sink.getName(), sinkName);
            assertEquals(sink.getNumOfShards().intValue(), Integer.parseInt(numOfShards));
            assertEquals(sink.getTempDir(), tempDir);
            assertEquals(sink.getLocation(), location);
            assertEquals(sink.getCreateSchema(), Boolean.parseBoolean(createSchema));

            BaseRegistry registry = sink.getRegistry();
            Assert.assertEquals(registry.getType(), HortonRegistry.TYPE);
            assertEquals(registry.getUrl(), registryUrl);

            if(registry instanceof HortonRegistry) {
                HortonRegistry hortonRegistry = (HortonRegistry)registry;
                assertEquals(hortonRegistry.getType(), HortonRegistry.TYPE);
                assertEquals(hortonRegistry.getUrl(), registryUrl);
                assertEquals(hortonRegistry.getSchemaGroup(), avroSchemaGroup);
            }

            List<FsSinkMapping> mapping = sink.getMapping().getList();
            assertFalse(mapping.isEmpty());
            assertEquals(1, mapping.size());

            FsSinkMapping map = mapping.get(0);
            assertNotNull(map);
            assertEquals(map.getTag(), datasetName);
            assertEquals(map.getSchema(), avroSchemaName);
            assertEquals(map.getPath(), datasetPath);
            assertEquals(map.getInferSchema(), Boolean.valueOf(inferSchema));
        }
    }
}