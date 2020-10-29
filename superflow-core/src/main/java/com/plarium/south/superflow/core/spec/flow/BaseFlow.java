package com.plarium.south.superflow.core.spec.flow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;

public abstract class BaseFlow extends RootElement {

    public BaseFlow(String name) {
        super(name);
    }

    public abstract void expand(Pipeline pipeline, PipelineOptions options);

}