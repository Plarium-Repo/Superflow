package com.plarium.south.superflow.core.spec.dofn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import static org.apache.beam.sdk.util.RowJson.RowJsonSerializer.forSchema;
import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

public class BeamRowToJson extends DoFn<Row, String> {

    private final Schema schema;

    private transient ObjectMapper mapper;


    public BeamRowToJson(Schema schema) {
        this.schema = schema;
    }

    @Setup
    public void setup() {
        mapper = newObjectMapperWith(forSchema(schema));
    }


    @ProcessElement
    public void process(@Element Row row, ProcessContext context) throws JsonProcessingException {
        context.output(mapper.writeValueAsString(row));
    }
}
