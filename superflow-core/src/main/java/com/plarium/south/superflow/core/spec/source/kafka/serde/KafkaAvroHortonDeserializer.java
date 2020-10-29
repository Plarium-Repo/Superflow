package com.plarium.south.superflow.core.spec.source.kafka.serde;

import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaAvroHortonDeserializer implements Deserializer<GenericRecord> {
    private KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        return toRecord(topic, data);
    }

    private GenericRecord toRecord(String topic, byte[] data) {
        return (GenericRecord) deserializer.deserialize(topic, data);
    }

    @Override
    public void close() {
        deserializer.close();
    }
}
