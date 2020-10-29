package com.plarium.south.superflow.core.spec.source;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.source.bigquery.BigQuerySource;
import com.plarium.south.superflow.core.spec.source.fs.AvroFileStoreSource;
import com.plarium.south.superflow.core.spec.source.hcatalog.HCatalogSource;
import com.plarium.south.superflow.core.spec.source.kafka.AvroKafkaSource;
import com.plarium.south.superflow.core.spec.source.kafka.JsonKafkaSource;
import com.plarium.south.superflow.core.spec.source.updated.jdbc.UpdatedJdbcSource;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = HCatalogSource.TYPE, value = HCatalogSource.class),
        @JsonSubTypes.Type(name = BigQuerySource.TYPE, value = BigQuerySource.class),
        @JsonSubTypes.Type(name = CompositeSource.TYPE, value = CompositeSource.class),
        @JsonSubTypes.Type(name = AvroKafkaSource.TYPE, value = AvroKafkaSource.class),
        @JsonSubTypes.Type(name = UpdatedJdbcSource.TYPE, value = UpdatedJdbcSource.class),
        @JsonSubTypes.Type(name = AvroFileStoreSource.TYPE, value = AvroFileStoreSource.class),
        @JsonSubTypes.Type(name = JsonKafkaSource.TYPE, value = JsonKafkaSource.class)
})
@JsonPropertyOrder({"name", "window", "registry"})
public abstract class BasePTupleRowSource extends BaseSource<Row, PCollectionTuple> {

    public BasePTupleRowSource(
            @Nullable String name,
            @Nullable BaseRegistry registry,
            @Nullable BaseWindow window)
    {
        super(name, registry, window);
    }
}
