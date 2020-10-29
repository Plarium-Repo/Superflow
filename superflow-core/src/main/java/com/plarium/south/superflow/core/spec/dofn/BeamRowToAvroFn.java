package com.plarium.south.superflow.core.spec.dofn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;


public class BeamRowToAvroFn extends DoFn<Row, GenericRecord> {

    private final Schema beamSchema;

    private org.apache.avro.Schema avroSchema;

    public BeamRowToAvroFn(Schema beamSchema) {
        this.beamSchema = beamSchema;
    }

    private org.apache.avro.Schema getAvroSchema() {
        if(avroSchema != null) return avroSchema;
        else return avroSchema = AvroUtils.toAvroSchema(beamSchema);
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<GenericRecord> out) {
        out.output(AvroUtils.toGenericRecord(row, getAvroSchema()));
    }
}