package com.plarium.south.superflow.core.spec.source.kafka.serde;

import com.plarium.south.superflow.core.utils.BeamRowUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

import static org.apache.beam.sdk.schemas.utils.AvroUtils.toBeamSchema;


public class KafkaBeamRowDeserializer implements Deserializer<Row>, AvroConfigFetcher {

    protected Schema avroSchema;

    protected Boolean allowNoStrict;

    protected Boolean schemaEvolution;

    protected org.apache.beam.sdk.schemas.Schema beamSchema;

    private KafkaAvroHortonDeserializer deserializer = new KafkaAvroHortonDeserializer();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
        avroSchema = fetchAvroSchema(configs);
        beamSchema = toBeamSchema(avroSchema);
        schemaEvolution = fetchSchemaEvo(configs);
        allowNoStrict = fetchAllowNoStrict(configs);
    }

    @Override
    public Row deserialize(String topic, byte[] avro) {
        GenericRecord record = deserializer.deserialize(topic, avro);
        return toBeamRow(record);
    }

    @Override
    public void close() {
        deserializer.close();
    }

    protected Row toBeamRow(GenericRecord record) {
        org.apache.beam.sdk.schemas.Schema schema =
                getBeamSchema(record.getSchema());

        return BeamRowUtils.toBeamRow(record, schema, allowNoStrict);
    }

    protected org.apache.beam.sdk.schemas.Schema getBeamSchema(Schema nextAvroSchema) {
        int h1 = avroSchema.hashCode();
        int h2 = nextAvroSchema.hashCode();

        if (!Objects.equals(h1, h2) && !schemaEvolution) {
            throw new AvroSchemaChangedException(avroSchema, nextAvroSchema);
        }

        return beamSchema;
    }
}
