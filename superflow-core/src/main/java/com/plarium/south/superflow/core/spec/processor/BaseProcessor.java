package com.plarium.south.superflow.core.spec.processor;

import com.plarium.south.superflow.core.spec.commons.BaseTransform;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import javax.annotation.Nullable;

public abstract class BaseProcessor<DataType, Input extends PInput, Output extends POutput> extends BaseTransform<DataType, Input, Output> {

    protected BaseProcessor(
            @Nullable String name,
            @Nullable BaseRegistry registry,
            @Nullable BaseWindow window)
    {
        super(name, registry, window);
    }

    public abstract String getType();

    @Override
    public void setup() {}

}
