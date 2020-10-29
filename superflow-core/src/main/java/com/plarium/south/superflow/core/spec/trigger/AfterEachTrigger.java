package com.plarium.south.superflow.core.spec.trigger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.TRIGGER;

@SchemaDefinition(
        type = TRIGGER,
        required = {"type", "triggers"},
        baseTypes = {"trigger"})

@JsonPropertyOrder({"type", "triggers"})
public class AfterEachTrigger extends BaseTrigger<Trigger> {
    public static final String TYPE = "trigger/after/each";

    @Getter @NotNull
    @JsonPropertyDescription("List of triggers")
    private final List<BaseTrigger<Trigger>> triggers;


    @JsonCreator
    public AfterEachTrigger(
            @JsonProperty(value = "triggers", required = true) List<BaseTrigger<Trigger>> triggers)
    {
        this.triggers = triggers;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected Trigger create() {
        return AfterEach.inOrder(fetchTriggers(triggers));
    }
}
