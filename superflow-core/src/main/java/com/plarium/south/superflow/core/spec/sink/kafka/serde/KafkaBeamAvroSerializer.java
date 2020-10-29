package com.plarium.south.superflow.core.spec.sink.kafka.serde;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaBeamAvroSerializer implements Serializer<GenericRecord> {
    public static final String MAP_TOPIC_TO_SCHEMA_KEY = "map.topic.to.schema";

    private Map<String, String> mapping;

    private KafkaAvroSerializer serializer = new KafkaAvroSerializer() {
        @Override
        public SchemaMetadata getSchemaKey(String topic, boolean isKey) {
            return super.getSchemaKey(mapping.get(topic), isKey);
        }
    };

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        mapping = (Map<String, String>) configs.get(MAP_TOPIC_TO_SCHEMA_KEY);
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        return serializer.serialize(topic, data);
    }

    @Override
    public void close() {
        serializer.close();
    }
}