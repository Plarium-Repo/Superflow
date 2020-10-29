package com.plarium.south.superflow.core.spec.dofn;

import com.plarium.south.superflow.core.utils.BeamRowUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class AvroToBeamRowFn extends DoFn<GenericRecord, Row> {

    private Schema beamSchema;

    private final Boolean allowNoStrict;


    public AvroToBeamRowFn(Schema beamSchema, Boolean allowNoStrict) {
        this.beamSchema = beamSchema;
        this.allowNoStrict = allowNoStrict;
    }

    @ProcessElement
    public void process(@Element GenericRecord record, OutputReceiver<Row> out) {
        out.output(BeamRowUtils.toBeamRow(record, beamSchema, allowNoStrict));
    }
}
