package com.plarium.south.superflow.core.spec.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.values.*;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SOURCE;
import static com.plarium.south.superflow.core.utils.PTupleUtils.materializeTags;

@SchemaDefinition(
        type = SOURCE,
        baseTypes = {"registry", "window"},
        required = {"type", "list"})

@JsonPropertyOrder({"type", "name", "registry", "window", "list"})
public class CompositeSource extends BasePTupleRowSource {
    public static final String TYPE = "source/list";

    @Getter @NotNull
    @JsonPropertyDescription("The list of sources")
    private final List<BasePTupleRowSource> list;


    public CompositeSource(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "list", required = true) List<BasePTupleRowSource> list)
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
            if (p.windowIsNeed()) p.setWindow(window);
            if (p.registryIsNeed()) p.setRegistry(registry);
            p.setup();
        });
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
        PCollectionTuple output = PCollectionTuple.empty(input.getPipeline());

        for (BasePTupleRowSource source : list) {
            PCollectionTuple outputPart = source.expand(input);
            Set<TupleTag<?>> tags = outputPart.getAll().keySet();

            for (TupleTag<Object> tag : materializeTags(tags)) {
                output = output.and(tag, outputPart.get(tag));
            }
        }

        return output;
    }
}
