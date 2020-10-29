package com.plarium.south.superflow.core.spec.sink.kafka.serde;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import static org.apache.beam.sdk.schemas.utils.AvroUtils.*;

@Slf4j
public class KafkaBeamRowSerializer implements Serializer<Row> {
    private KafkaBeamAvroSerializer serializer = new KafkaBeamAvroSerializer();

    private Schema avroSchema;


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Row row) {
        Schema avroSchema = getAvroSchema(row.getSchema());
        GenericRecord record = toGenericRecord(row, avroSchema);
        return serializer.serialize(topic, record);
    }

    private Schema getAvroSchema(org.apache.beam.sdk.schemas.Schema nextSchema) {
        if (avroSchema == null) {
            avroSchema = toAvroSchema(nextSchema);
        }

        return avroSchema;
    }

    @Override
    public void close() {
        serializer.close();
    }
}
