package com.plarium.south.superflow.core.spec.dofn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class AvroToJsonStringFn extends DoFn<GenericRecord, String> {

    @ProcessElement
    public void process(@Element GenericRecord record, OutputReceiver<String> out) {
        out.output(record.toString());
    }
}
