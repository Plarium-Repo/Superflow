package com.plarium.south.superflow.core.spec.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.bigtable.v2.Row;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.trigger.BaseTrigger;
import com.plarium.south.superflow.core.utils.DurationUtils;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.WINDOW;

@SchemaDefinition(
        type = WINDOW,
        required = {"type", "size", "newEvery"},
        baseTypes = {"trigger"})

@JsonPropertyOrder({"type", "allowedLateness", "accumulateWindow", "timeCombiner", "size", "newEvery", "trigger"})
public class SlidingWindow extends BaseWindow {
    public static final String TYPE = "window/sliding";

    @Getter @NotNull
    @JsonPropertyDescription("Window size: <number <ms|sec|min|hour|day>>")
    private final String size;

    @Getter @NotNull
    @JsonPropertyDescription("Create new window every: <number <ms|sec|min|hour|day>>")
    private final String newEvery;

    @JsonCreator
    public SlidingWindow(
            @JsonProperty(value = "size", required = true) String size,
            @JsonProperty(value = "newEvery", required = true) String newEvery,
            @JsonProperty(value = "timeCombiner") TimeCombiner timeCombiner,
            @JsonProperty(value = "allowedLateness") String allowedLateness,
            @JsonProperty(value = "accumulateWindow") Boolean accumulateWindow,
            @JsonProperty(value = "trigger") BaseTrigger<Trigger> trigger)
    {
        super(allowedLateness, timeCombiner, accumulateWindow, trigger);
        this.size = size;
        this.newEvery = newEvery;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public <T> Window<T> create() {
        return Window.into(SlidingWindows.of(DurationUtils.parse(size))
                .every(DurationUtils.parse(newEvery)));
    }
}
