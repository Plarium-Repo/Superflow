package com.plarium.south.superflow.core.spec.commons;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import com.plarium.south.superflow.core.utils.WindowUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.springframework.util.Assert;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public abstract class BaseTransform<DataType, Input extends PInput, Output extends POutput> extends PTransform<Input, Output> implements SetupSpec {

    @Getter @Setter @NotNull
    @JsonPropertyDescription("The schema registry")
    protected BaseRegistry registry;

    @Getter @Setter
    @JsonPropertyDescription("The window for apply on transform")
    protected BaseWindow window;


    public BaseTransform(
            @Nullable String name,
            @Nullable BaseRegistry registry,
            @Nullable BaseWindow window)
    {
        super(name);
        this.window = window;
        this.registry = registry;
    }

    public boolean windowIsNeed() {
        return window == null;
    }

    public boolean registryIsNeed() {
        return registry == null;
    }


    protected <T> PCollection<T> applyWindow(PCollection<T> input) {
        return window == null ? input : window.apply(input);
    }

    protected PCollection<DataType> getByTag(PCollectionTuple input, String tag) {
        Assert.isTrue(input.has(tag), "Not found tag in pipeline with name: \"" + tag + "\"");	
        return input.get(tag);
    }

    protected PCollection<Row> applyTimePolicy(PCollection<Row> dataset, String fieldName) {
        if (fieldName == null) return dataset;
        return WindowUtils.applyTimePolicy(dataset, fieldName);
    }
}
