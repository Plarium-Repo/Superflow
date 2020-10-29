package com.plarium.south.superflow.core.spec.source.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.registry.HortonRegistry;
import com.plarium.south.superflow.core.spec.source.mapping.TopicSourceAvroMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonKafkaSourceTest {


    private String kafkaSourceName = "source-name";
    private String kafkaOffsetReset = "earliest";
    private String kafkaAutoCommit = "false";
    private String kafkaDelayCommit = "3 sec";
    private String kafkaGroupId = "kafka-group-id";
    private String kafkaBootstrap = "host1:9092,host2:9092";
    private String kafkaDatasetName = "dataset-name";
    private String avroSchemaVersion = "1";
    private String avroSchemaName = "avroSchemaName";
    private String eventTime = "timestamp";
    private String kafkaTopicName = "kafkaTopicName";
    private String kafkaMaxPollRecords = "1000";
    private String registryUrl = "http://endpoint/v1/api";

    protected Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("kafka.source.name", kafkaSourceName);
        env.put("kafka.offset.reset", kafkaOffsetReset);
        env.put("kafka.auto.commit", kafkaAutoCommit);
        env.put("kafka.delay.commit", kafkaDelayCommit);
        env.put("kafka.group.id", kafkaGroupId);
        env.put("kafka.bootstrap", kafkaBootstrap);
        env.put("kafka.dataset.name", kafkaDatasetName);
        env.put("avro.schema.version", avroSchemaVersion);
        env.put("avro.schema.name", avroSchemaName);
        env.put("event.time.field", eventTime);
        env.put("kafka.topic.name", kafkaTopicName);
        env.put("kafka.max.poll.records", kafkaMaxPollRecords);
        env.put("registry.url", registryUrl);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sources/kafka/json-kafka-source.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<JsonKafkaSource> type = new TypeReference<JsonKafkaSource>() {};
        List<JsonKafkaSource> sources = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (JsonKafkaSource kafkaSource: sources) {
            kafkaSource.setup();

            assertEquals(kafkaSource.getType(), JsonKafkaSource.TYPE);
            assertEquals(kafkaSource.getName(), kafkaSourceName);
            assertEquals(kafkaSource.getOffset(), kafkaOffsetReset);
            assertEquals(kafkaSource.getAutoCommit(), Boolean.getBoolean(kafkaAutoCommit));
            assertEquals(kafkaSource.getGroupId(), kafkaGroupId);
            assertEquals(kafkaSource.getBootstrap(), kafkaBootstrap);

            BaseRegistry registry = kafkaSource.getRegistry();
            Assert.assertEquals(registry.getType(), HortonRegistry.TYPE);
            assertEquals(registry.getUrl(), registryUrl);

            if(registry instanceof HortonRegistry) {
                HortonRegistry hortonRegistry = (HortonRegistry)registry;
                assertEquals(hortonRegistry.getType(), HortonRegistry.TYPE);
                assertEquals(hortonRegistry.getUrl(), registryUrl);

                Map<String, Integer> versions = hortonRegistry.getVersions();
                Integer version = new ArrayList<>(versions.values()).get(0);
                assertEquals(version.intValue(), Integer.parseInt(avroSchemaVersion));
            }
            MappingList<TopicSourceAvroMapping> sourceMapping = kafkaSource.getMapping();
            assertNotNull(sourceMapping);
            List<TopicSourceAvroMapping> mapping = sourceMapping.getList();
            assertFalse(mapping.isEmpty());
            assertEquals(sourceMapping.getEventTime(), eventTime);

            TopicSourceAvroMapping map = mapping.get(0);
            assertEquals(map.getTopic(), kafkaTopicName);
            assertEquals(map.getSchema(), avroSchemaName);
            assertEquals(map.getTag(), kafkaDatasetName);
            assertEquals(map.getEventTime(), eventTime);
            assertEquals(map.getDelayCommit(), kafkaDelayCommit);
            assertEquals(map.getAutoCommit(), Boolean.valueOf(kafkaAutoCommit));

            Map<String, Object> config = kafkaSource.getKafkaConfig();
            Integer maxPoolRecords = (Integer) config.get("max.poll.records");
            assertEquals(maxPoolRecords.intValue(), Integer.parseInt(kafkaMaxPollRecords));
        }
    }

}