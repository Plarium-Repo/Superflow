package com.plarium.south.superflow.core.spec.trigger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.TRIGGER;

@SchemaDefinition(
        type = TRIGGER,
        required = {"type", "elementCount"})

@JsonPropertyOrder({"type", "elementCount"})
public class AfterPaneTrigger extends BaseTrigger<Trigger> {
    public static final String TYPE = "trigger/after/pane";

    @Getter @NotNull
    @JsonPropertyDescription("Fires when the pane contains at least elements")
    private final Integer elementCount;


    public AfterPaneTrigger(
            @JsonProperty(value = "elementCount", required = true) Integer elementCount)
    {
        this.elementCount = elementCount;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected Trigger create() {
        return AfterPane.elementCountAtLeast(elementCount);
    }
}
