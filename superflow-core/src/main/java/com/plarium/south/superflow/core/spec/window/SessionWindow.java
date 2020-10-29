package com.plarium.south.superflow.core.spec.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.trigger.BaseTrigger;
import com.plarium.south.superflow.core.utils.DurationUtils;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.WINDOW;

@SchemaDefinition(
        type = WINDOW,
        required = {"type", "gapSize"},
        baseTypes = {"trigger"})

@JsonPropertyOrder({"type", "allowedLateness", "accumulateWindow", "timeCombiner", "gapSize", "trigger"})
public class SessionWindow extends BaseWindow {
    public static final String TYPE = "window/session";

    @Getter
    @NotNull
    @JsonPropertyDescription("Window gap size: <number <ms|sec|min|hour|day>>")
    private final String gapSize;

    @JsonCreator
    public SessionWindow(
            @JsonProperty(value = "gapSize", required = true) String gapSize,
            @JsonProperty(value = "timeCombiner") TimeCombiner timeCombiner,
            @JsonProperty(value = "allowedLateness") String allowedLateness,
            @JsonProperty(value = "accumulateWindow") Boolean accumulateWindow,
            @JsonProperty(value = "trigger") BaseTrigger<Trigger> trigger)
    {
        super(allowedLateness, timeCombiner, accumulateWindow, trigger);
        this.gapSize = gapSize;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public <T> Window<T> create() {
        return Window.into(Sessions.withGapDuration(DurationUtils.parse(gapSize)));
    }
}
