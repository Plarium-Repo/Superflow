package com.plarium.south.superflow.core.spec.sink.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.registry.HortonRegistry;
import com.plarium.south.superflow.core.spec.sink.mapping.TopicSinkMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class AvroKafkaTupleSinkTest extends TestSpecUtils {

    private String sinkName = "sink-name";
    private String kafkaBootstrap = "host1:9092,host2:9092";
    private String registryUrl = "http://endpoint/v1/api";
    private String avroSchemaGroup = "avroSchemaGroup";
    private String avroSchemaName = "avroSchemaName";
    private String kafkaTopicName = "kafkaTopicName";
    private String kafkaDatasetName = "dataset-name";
    private String kafkaMaxPollRecords = "1000";

    protected Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("sink.name", sinkName);
        env.put("kafka.bootstrap", kafkaBootstrap);
        env.put("kafka.dataset.name", kafkaDatasetName);
        env.put("avro.schema.name", avroSchemaName);
        env.put("kafka.topic.name", kafkaTopicName);
        env.put("registry.url", registryUrl);
        env.put("avro.schema.group", avroSchemaGroup);
        env.put("kafka.max.poll.records", kafkaMaxPollRecords);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sinks/kafka/avro-kafka-sink.yaml";
        String yamlTemplated = renderTemplate(path, getEnv());
        TypeReference<AvroKafkaTupleSink> type = new TypeReference<AvroKafkaTupleSink>() {};
        List<AvroKafkaTupleSink> sinks = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (AvroKafkaTupleSink sink : sinks) {
            sink.setup();

            assertEquals(sink.getType(), AvroKafkaTupleSink.TYPE);
            assertEquals(sink.getName(), sinkName);
            assertEquals(sink.getBootstrap(), kafkaBootstrap);

            BaseRegistry registry = sink.getRegistry();
            Assert.assertEquals(registry.getType(), HortonRegistry.TYPE);
            assertEquals(registry.getUrl(), registryUrl);

            if(registry instanceof HortonRegistry) {
                HortonRegistry hortonRegistry = (HortonRegistry)registry;
                assertEquals(hortonRegistry.getType(), HortonRegistry.TYPE);
                assertEquals(hortonRegistry.getUrl(), registryUrl);
                assertEquals(hortonRegistry.getSchemaGroup(), avroSchemaGroup);
            }

            List<TopicSinkMapping> mapping = sink.getMapping().getList();
            assertFalse(mapping.isEmpty());

            TopicSinkMapping map = mapping.get(0);
            assertEquals(map.getTag(), kafkaDatasetName);
            assertEquals(map.getSchema(), avroSchemaName);
            assertEquals(map.getTopic(), kafkaTopicName);

            Map<String, Object> config = sink.getKafkaConfig();
            Integer maxPoolRecords = (Integer) config.get("max.poll.records");
            assertEquals(maxPoolRecords.intValue(), Integer.parseInt(kafkaMaxPollRecords));
        }
    }
}