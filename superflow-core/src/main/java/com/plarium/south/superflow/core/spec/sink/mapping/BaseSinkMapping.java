package com.plarium.south.superflow.core.spec.sink.mapping;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.plarium.south.superflow.core.spec.mapping.BaseMapping;
import lombok.Getter;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.firstNonNull;

public class BaseSinkMapping extends BaseMapping {

    @Getter
    @JsonPropertyDescription("The schema name for create it in registry if needed")
    private final String schema;

    @Getter
    @JsonPropertyDescription("Infer schema from dataset or get from registry, default: true")
    private final Boolean inferSchema;

    @Getter
    @JsonPropertyDescription("The avro schema namespace (using for create or update schema in registry)")
    private final String namespace;


    public BaseSinkMapping(
            String tag,
            @Nullable String schema,
            @Nullable String eventTime,
            @Nullable Boolean inferSchema,
            @Nullable String namespace)
    {
        super(tag, eventTime);
        this.schema = schema;
        this.inferSchema = firstNonNull(inferSchema, true);
        this.namespace = namespace;
    }
}
