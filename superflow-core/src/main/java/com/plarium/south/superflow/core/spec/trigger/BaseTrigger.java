package com.plarium.south.superflow.core.spec.trigger;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = AfterAllTrigger.TYPE, value = AfterAllTrigger.class),
        @JsonSubTypes.Type(name = AfterEachTrigger.TYPE, value = AfterEachTrigger.class),
        @JsonSubTypes.Type(name = AfterPaneTrigger.TYPE, value = AfterPaneTrigger.class),
        @JsonSubTypes.Type(name = AfterFirstTrigger.TYPE, value = AfterFirstTrigger.class),
        @JsonSubTypes.Type(name = RepeatedlyTrigger.TYPE, value = RepeatedlyTrigger.class),
        @JsonSubTypes.Type(name = AfterProcTimeTrigger.TYPE, value = AfterProcTimeTrigger.class),
        @JsonSubTypes.Type(name = AfterWatermarkTrigger.TYPE, value = AfterWatermarkTrigger.class)
})
public abstract class BaseTrigger<T extends Trigger> implements Serializable {

    public abstract String getType();

    protected abstract T create();


    public <DataType> Window<DataType> apply(Window<DataType> window) {
        return window.triggering(create());
    }

    protected List<Trigger> fetchTriggers(List<BaseTrigger<Trigger>> triggers) {
        return triggers.stream()
                .map(BaseTrigger::create)
                .collect(Collectors.toList());
    }
}