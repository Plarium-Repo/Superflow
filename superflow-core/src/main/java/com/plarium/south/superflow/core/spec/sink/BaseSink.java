package com.plarium.south.superflow.core.spec.sink;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.plarium.south.superflow.core.spec.commons.BaseTransform;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.springframework.util.Assert;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.firstNonNull;

public abstract class BaseSink<DataType, Input extends PInput> extends BaseTransform<DataType, Input, PDone> {

    @Getter
    @JsonPropertyDescription("Create output schema in registry")
    protected final Boolean createSchema;

    protected BaseSink(
            @Nullable String name,
            @Nullable BaseRegistry registry,
            @Nullable Boolean createSchema,
            @Nullable BaseWindow window)
    {

        super(name, registry, window);
        this.createSchema = firstNonNull(createSchema, false);
    }

    public abstract String getType();

    @Override
    public void setup() {}

    protected void createSchemaIfNeeded(String schemaName, String schema) {
        Assert.notNull(schema, "The schema is required");
        if (schemaName == null || !createSchema) return;
        registry.createSchema(schemaName, schema);
    }
}
