package com.plarium.south.superflow.core.spec.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.trigger.BaseTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;


import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.WINDOW;

@SchemaDefinition(
        type = WINDOW,
        required = {"type"},
        baseTypes = {"trigger"})

@JsonPropertyOrder({"type", "accumulateWindow", "trigger"})
public class GlobalWindow extends BaseWindow {
    public static final String TYPE = "window/global";

    @JsonCreator
    public GlobalWindow(
            @JsonProperty(value = "accumulateWindow") Boolean accumulateWindow,
            @JsonProperty(value = "trigger") BaseTrigger<Trigger> trigger)
    {
        super(null, null, accumulateWindow, trigger);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected <T> Window<T> create() {
        return Window.into(new GlobalWindows());
    }
}


