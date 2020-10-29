package com.plarium.south.superflow.core.spec.trigger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.TRIGGER;

@SchemaDefinition(
        type = TRIGGER,
        required = {"type", "repeated"})

@JsonPropertyOrder({"type", "repeated"})
public class RepeatedlyTrigger extends BaseTrigger<Trigger> {
    public static final String TYPE = "trigger/repeated";

    @Getter @NotNull
    @JsonPropertyDescription("The repeatedly trigger")
    private final BaseTrigger<Trigger> repeated;

    @JsonCreator
    public RepeatedlyTrigger(
            @JsonProperty(value = "repeated", required = true) BaseTrigger<Trigger> repeated)
    {
        this.repeated = repeated;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected Trigger create() {
        return Repeatedly.forever(repeated.create());
    }
}
