package com.plarium.south.superflow.core.spec.trigger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.TRIGGER;

@SchemaDefinition(
        type = TRIGGER,
        required = "type")

public class AfterWatermarkTrigger extends BaseTrigger<Trigger> {
    public static final String TYPE = "trigger/after/watermark";

    @Getter @NotNull
    @JsonPropertyDescription("Trigger fires before the watermark has passed the end of the window")
    private final BaseTrigger<OnceTrigger> early;

    @Getter @NotNull
    @JsonPropertyDescription("Trigger fires after the watermark has passed the end of the window")
    private final BaseTrigger<OnceTrigger> late;


    @JsonCreator
    public AfterWatermarkTrigger(
            @JsonProperty(value = "early") BaseTrigger<OnceTrigger> early,
            @JsonProperty(value = "late") BaseTrigger<OnceTrigger> late)
    {
        this.early = early;
        this.late = late;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected Trigger create() {
        AfterWatermark.FromEndOfWindow trigger = AfterWatermark.pastEndOfWindow();

        AfterWatermarkEarlyAndLate earlyAndLate = null;

        if (early != null) {
            earlyAndLate = trigger.withEarlyFirings(early.create());
        }

        if (late != null) {
            earlyAndLate = trigger.withLateFirings(late.create());
        }

        return earlyAndLate != null ? earlyAndLate : trigger;
    }
}
