package com.plarium.south.superflow.core.spec.source.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Map;

import static org.apache.beam.sdk.schemas.utils.AvroUtils.toBeamSchema;
import static org.apache.beam.sdk.util.RowJson.RowJsonDeserializer.forSchema;
import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

@Slf4j
public class KafkaJsonToBeamDeserializer implements Deserializer<Row>, AvroConfigFetcher {

    private ObjectMapper objectMapper;

    private StringDeserializer deserializer = new StringDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);

        Schema avroSchema = fetchAvroSchema(configs);
        org.apache.beam.sdk.schemas.Schema beamSchema = toBeamSchema(avroSchema);
        objectMapper = newObjectMapperWith(forSchema(beamSchema));
    }

    @Override
    public Row deserialize(String topic, byte[] data) {
        String jsonString = deserializer.deserialize(topic, data);

        try {
            return objectMapper.readValue(jsonString, Row.class);
        }
        catch (IOException e) {
            throw new KafkaJsonToBeamDeserializerException(e, jsonString);
        }
    }

    @Override
    public void close() {
        deserializer.close();
    }

    private static class KafkaJsonToBeamDeserializerException extends RuntimeException {
        private static final String ERROR_MSG = "Unable convert json to beam row: %s";

        public KafkaJsonToBeamDeserializerException(Throwable throwable, String json) {
            super(String.format(ERROR_MSG, json), throwable);
        }
    }
}
