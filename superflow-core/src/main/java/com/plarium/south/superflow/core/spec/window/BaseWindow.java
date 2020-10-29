package com.plarium.south.superflow.core.spec.window;

import com.fasterxml.jackson.annotation.*;
import com.plarium.south.superflow.core.spec.trigger.BaseTrigger;
import lombok.Getter;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.io.Serializable;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.utils.DurationUtils.parse;
import static org.apache.beam.sdk.transforms.windowing.TimestampCombiner.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = FixedWindow.TYPE, value = FixedWindow.class),
        @JsonSubTypes.Type(name = GlobalWindow.TYPE, value = GlobalWindow.class),
        @JsonSubTypes.Type(name = SessionWindow.TYPE, value = SessionWindow.class),
        @JsonSubTypes.Type(name = SlidingWindow.TYPE, value = SlidingWindow.class)
})
@JsonPropertyOrder({"type", "allowedLateness", "accumulateWindow", "timeCombiner", "trigger"})
public abstract class BaseWindow implements Serializable {

    @Getter
    @JsonPropertyDescription("Window allowed lateness data: <number <ms|sec|min|hour|day>>")
    protected final String allowedLateness;

    @Getter
    @JsonPropertyDescription("Accumulates elements in a windows after they are triggered")
    protected final Boolean accumulateWindow;

    @Getter
    @JsonPropertyDescription("Window timestamp combiner strategy for aggregation")
    protected final TimeCombiner timeCombiner;

    @Getter
    @JsonPropertyDescription("Window trigger")
    protected final BaseTrigger<Trigger> trigger;


    public BaseWindow(
            @Nullable String allowedLateness,
            @Nullable TimeCombiner timeCombiner,
            @Nullable Boolean accumulateWindow,
            @Nullable BaseTrigger<Trigger> trigger)
    {
        this.trigger = trigger;
        this.timeCombiner = timeCombiner;
        this.allowedLateness = allowedLateness;
        this.accumulateWindow = firstNonNull(accumulateWindow, false);
    }


    public abstract String getType();

    protected abstract <T> Window<T> create();


    public <T> PCollection<T> apply(PCollection<T> input) {
        Window<T> window = create();

        if (allowedLateness != null) {
            window = window.withAllowedLateness(parse(allowedLateness));
        }

        if (getTimeCombiner() != null) {
            switch (getTimeCombiner()) {
                case earliest:
                    window = window.withTimestampCombiner(EARLIEST);
                    break;
                case latest:
                    window = window.withTimestampCombiner(LATEST);
                    break;
                case end_of_window:
                    window = window.withTimestampCombiner(END_OF_WINDOW);
                    break;
                default:
                    String errorMsg = "Illegal timeCombiner value: " + getTimeCombiner();
                    throw new IllegalArgumentException(errorMsg);
            }
        }

        if (trigger != null) {
            window = trigger.apply(window);
        }

        window = accumulateWindow ?
                window.accumulatingFiredPanes() :
                window.discardingFiredPanes();

        return input.apply(window);
    }
}