package com.plarium.south.superflow.core.spec.processor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.values.PCollectionTuple;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.beam.sdk.values.Row;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;

@SchemaDefinition(
        type = PROCESSOR,
        required = {"type", "list"},
        baseTypes = {"registry", "window"})

@JsonPropertyOrder({"type", "name", "registry", "window", "list"})
public class CompositeProcessor extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/list";

    @Getter @NotNull @NotEmpty
    @JsonPropertyDescription("The list of processors")
    private final List<BasePTupleRowProcessor> list;

    @JsonCreator
    public CompositeProcessor(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "list" ,required = true) List<BasePTupleRowProcessor> list)
    {
        super(name, registry, window);
        this.list = list;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void setup() {
        list.forEach(p -> {
            if (p.getWindow() == null) p.setWindow(window);
            if (p.getRegistry() == null) p.setRegistry(registry);
            p.setup();
        });
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        for (BasePTupleRowProcessor processor : list) {
            input = processor.expand(input);
        }

        return input;
    }
}
