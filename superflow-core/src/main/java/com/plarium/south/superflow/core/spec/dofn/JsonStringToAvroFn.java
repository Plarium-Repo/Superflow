package com.plarium.south.superflow.core.spec.dofn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.*;

public class JsonStringToAvroFn extends DoFn<String, GenericRecord> {

    private transient Schema avroSchema;

    private final String avroSchemaString;


    public JsonStringToAvroFn(String avroSchemaString) {
        this.avroSchemaString = avroSchemaString;
    }

    @ProcessElement
    public void process(@Element String json, OutputReceiver<GenericRecord> out) throws IOException {
        Decoder decoder = DecoderFactory.get().jsonDecoder(getAvroSchema(), json);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(getAvroSchema());
        GenericRecord record = reader.read(null, decoder);
        out.output(record);
    }

    private Schema getAvroSchema() {
        if (avroSchema == null) {
            avroSchema = new Schema.Parser().parse(avroSchemaString);
        }

        return avroSchema;
    }
}
