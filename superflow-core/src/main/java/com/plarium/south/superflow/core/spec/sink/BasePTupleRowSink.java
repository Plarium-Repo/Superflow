package com.plarium.south.superflow.core.spec.sink;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.bigquery.BigQuerySink;
import com.plarium.south.superflow.core.spec.sink.fs.AvroFileStoreSink;
import com.plarium.south.superflow.core.spec.sink.http.HttpSink;
import com.plarium.south.superflow.core.spec.sink.jdbc.JdbcSink;
import com.plarium.south.superflow.core.spec.sink.kafka.AvroKafkaTupleSink;
import com.plarium.south.superflow.core.spec.sink.mapping.BaseSinkMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = HttpSink.TYPE, value = HttpSink.class),
        @JsonSubTypes.Type(name = JdbcSink.TYPE, value = JdbcSink.class),
        @JsonSubTypes.Type(name = BigQuerySink.TYPE, value = BigQuerySink.class),
        @JsonSubTypes.Type(name = CompositeSink.TYPE, value = CompositeSink.class),
        @JsonSubTypes.Type(name = AvroFileStoreSink.TYPE, value = AvroFileStoreSink.class),
        @JsonSubTypes.Type(name = AvroKafkaTupleSink.TYPE, value = AvroKafkaTupleSink.class)
})
@JsonPropertyOrder({"name", "createSchema", "window", "registry"})
public abstract class BasePTupleRowSink extends BaseSink<Row, PCollectionTuple> {


    protected BasePTupleRowSink(
            @Nullable String name,
            @Nullable BaseRegistry registry,
            @Nullable Boolean createSchema,
            @Nullable BaseWindow window)
    {
        super(name, registry, createSchema, window);
    }


    protected String inferOrFetchSchema(BaseSinkMapping map, PCollection<Row> dataset) {
        if (map.getInferSchema()) {
            return AvroUtils.toAvroSchema(dataset.getSchema()).toString();
        }

        return registry.getSchema(map.getSchema());
    }
}
