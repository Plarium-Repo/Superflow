package com.plarium.south.superflow.core.spec.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.trigger.BaseTrigger;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.Row;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.WINDOW;
import static com.plarium.south.superflow.core.utils.DurationUtils.parse;

@SchemaDefinition(
        type = WINDOW,
        required = {"type", "size"},
        baseTypes = {"trigger"})

@JsonPropertyOrder({"type", "allowedLateness", "accumulateWindow", "timeCombiner", "size", "trigger"})
public class FixedWindow extends BaseWindow {
    public static final String TYPE = "window/fixed";

    @Getter @NotNull
    @JsonPropertyDescription("Window size: <number <ms|sec|min|hour|day>>")
    private final String size;

    @JsonCreator
    public FixedWindow(
            @JsonProperty(value = "size", required = true) String size,
            @JsonProperty(value = "timeCombiner") TimeCombiner timeCombiner,
            @JsonProperty(value = "allowedLateness") String allowedLateness,
            @JsonProperty(value = "accumulateWindow") Boolean accumulateWindow,
            @JsonProperty(value = "trigger") BaseTrigger<Trigger> trigger)
    {
        super(allowedLateness, timeCombiner, accumulateWindow, trigger);
        this.size = size;
    }


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public <T> Window<T> create() {
        return Window.into(FixedWindows.of(parse(size)));
    }
}
