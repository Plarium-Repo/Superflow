package com.plarium.south.superflow.core.spec.processor.enrich;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.plarium.south.superflow.core.spec.processor.enrich.jdbc.JdbcEnricher;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = JdbcEnricher.TYPE, value = JdbcEnricher.class),
})
public abstract class BaseEnricher implements Serializable {


    public abstract String getType();

    public abstract Schema querySchema();

    public abstract PCollection<Row> expand(Schema mergeSchema, PCollection<Row> input);
}
