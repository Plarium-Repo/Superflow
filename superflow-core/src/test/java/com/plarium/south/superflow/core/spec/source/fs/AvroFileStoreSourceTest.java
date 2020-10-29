package com.plarium.south.superflow.core.spec.source.fs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.registry.HortonRegistry;
import com.plarium.south.superflow.core.spec.source.mapping.FsSourceMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class AvroFileStoreSourceTest {

    private String sourceName = "avro-fs-source";
    private String sourceLocation = "/tmp/input";
    private String allowNoStrict = "true";
    private String registryUrl = "http://endpoint/v1/api";
    private String avroSchemaVersion = "1";
    private String avroSchemaName = "avroSchemaName";
    private String eventTime = "timestamp";
    private String datasetName = "dataset-name";
    private String datasetPath = "/dataset/**";



    protected Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("source.name", sourceName);
        env.put("source.location", sourceLocation);
        env.put("registry.url", registryUrl);
        env.put("avro.schema.name", avroSchemaName);
        env.put("avro.schema.version", avroSchemaVersion);
        env.put("event.time.field", eventTime);
        env.put("tuple.tag", datasetName);
        env.put("dataset.path", datasetPath);
        env.put("allow.no.strict", allowNoStrict);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sources/fs/avro-fs-source.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<AvroFileStoreSource> type = new TypeReference<AvroFileStoreSource>() {};
        List<AvroFileStoreSource> sources = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (AvroFileStoreSource source : sources) {
            source.setup();

            Assert.assertEquals(source.getName(), sourceName);
            assertEquals(source.getLocation(), sourceLocation);
            assertEquals(source.getAllowNoStrict(), Boolean.parseBoolean(allowNoStrict));


            BaseRegistry registry = source.getRegistry();
            Assert.assertEquals(registry.getType(), HortonRegistry.TYPE);
            assertEquals(registry.getUrl(), registryUrl);

            if(registry instanceof HortonRegistry) {
                HortonRegistry hortonRegistry = (HortonRegistry)registry;
                assertEquals(hortonRegistry.getType(), HortonRegistry.TYPE);
                assertEquals(hortonRegistry.getUrl(), registryUrl);

                Map<String, Integer> versions = hortonRegistry.getVersions();

                if(versions != null) {
                    Integer version = new ArrayList<>(versions.values()).get(0);
                    assertEquals(version.intValue(), Integer.parseInt(avroSchemaVersion));
                }
            }
            MappingList<FsSourceMapping> sourceMapping = source.getMapping();
            assertNotNull(sourceMapping);
            sourceMapping.setup();
            assertEquals(sourceMapping.getEventTime(), eventTime);

            List<FsSourceMapping> mapping = sourceMapping.getList();
            assertFalse(mapping.isEmpty());
            assertEquals(1, mapping.size());

            FsSourceMapping map = mapping.get(0);
            assertNotNull(map);
            assertEquals(map.getTag(), datasetName);
            assertEquals(map.getSchema(), avroSchemaName);
            assertEquals(map.getPath(), datasetPath);
            assertEquals(map.getEventTime(), eventTime);
        }
    }
}