package com.plarium.south.superflow.core.spec.source;

import com.plarium.south.superflow.core.spec.commons.BaseTransform;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.POutput;

import javax.annotation.Nullable;

public abstract class BaseSource<DataType, Output extends POutput> extends BaseTransform<DataType, PBegin, Output> {


    protected BaseSource(
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
