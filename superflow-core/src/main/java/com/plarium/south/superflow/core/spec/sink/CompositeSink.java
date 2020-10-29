package com.plarium.south.superflow.core.spec.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SINK;

@SchemaDefinition(
        type = SINK,
        baseTypes = {"registry", "window"},
        required = {"type", "list"})

@JsonPropertyOrder({"type", "name", "registry", "window", "list"})
public class CompositeSink extends BasePTupleRowSink {
    public static final String TYPE = "sink/list";

    @Getter @NotEmpty
    @JsonPropertyDescription("The list of sinks")
    private final List<BasePTupleRowSink> list;

    public CompositeSink(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "list", required = true) List<BasePTupleRowSink> list)
    {
        super(name, registry, false, window);
        this.list = list;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void setup() {
        list.forEach(p -> {
            if (p.windowIsNeed()) p.setWindow(window);
            if (p.registryIsNeed()) p.setRegistry(registry);
            p.setup();
        });
    }

    @Override
    public PDone expand(PCollectionTuple input) {
        for (BasePTupleRowSink sink : list) {
            sink.expand(input);
        }

        return PDone.in(input.getPipeline());
    }
}
