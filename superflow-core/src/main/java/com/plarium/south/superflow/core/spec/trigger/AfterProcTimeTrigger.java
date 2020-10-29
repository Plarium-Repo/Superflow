package com.plarium.south.superflow.core.spec.trigger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.TRIGGER;
import static com.plarium.south.superflow.core.utils.DurationUtils.parse;

@SchemaDefinition(
        type = TRIGGER,
        required = "type")

@JsonPropertyOrder({"type", "delay", "align"})
public class AfterProcTimeTrigger extends BaseTrigger<Trigger> {
    public static final String TYPE = "trigger/after/process";

    @Getter @NotNull
    @JsonPropertyDescription("The delay before fired accumulated data, format: 'number <sec|min|hour|day>")
    private final String delay;

    @Getter @NotNull
    @JsonPropertyDescription("Aligns the time to be the smallest multiple of period greater than the epoch, format: 'number <sec|min|hour|day>")
    private final String align;


    public AfterProcTimeTrigger(
            @JsonProperty(value = "delay") String delay,
            @JsonProperty(value = "align") String align)
    {
        this.delay = delay;
        this.align = align;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected Trigger create() {
        AfterProcessingTime trigger = AfterProcessingTime.pastFirstElementInPane();

        if (delay != null) {
            trigger = trigger.plusDelayOf(parse(delay));
        }

        if (align != null) {
            trigger = trigger.alignedTo(parse(align));
        }

        return trigger;
    }
}
