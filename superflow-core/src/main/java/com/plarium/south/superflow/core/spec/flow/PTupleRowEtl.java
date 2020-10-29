package com.plarium.south.superflow.core.spec.flow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Lists;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.commons.schema.SpecType;
import com.plarium.south.superflow.core.spec.processor.BasePTupleRowProcessor;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.BasePTupleRowSink;
import com.plarium.south.superflow.core.spec.source.BasePTupleRowSource;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import com.plarium.south.superflow.core.utils.PTupleUtils;
import com.plarium.south.superflow.core.options.SpecOptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;

import javax.validation.constraints.NotNull;

@Slf4j

@SchemaDefinition(
        type = SpecType.FLOW,
        baseTypes = {"registry", "window"},
        required = {"type", "source", "processor", "sink"})

@JsonPropertyOrder({"type", "name", "registry", "window", "source", "processor", "sink"})
public class PTupleRowEtl extends BaseFlow {
    public static final String TYPE = "flow/etl";

    @Getter @NotNull
    @JsonPropertyDescription("The default registry")
    protected final BaseRegistry registry;

    @Getter @NotNull
    @JsonPropertyDescription("The default window")
    protected final BaseWindow window;

    @Getter @NotNull
    @JsonPropertyDescription("The input source component")
    protected final BasePTupleRowSource source;

    @Getter @NotNull
    @JsonPropertyDescription("The processor component")
    protected final BasePTupleRowProcessor processor;

    @Getter @NotNull
    @JsonPropertyDescription("The sink component")
    protected final BasePTupleRowSink sink;


    @JsonCreator
    public PTupleRowEtl(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "source", required = true) BasePTupleRowSource source,
            @JsonProperty(value = "processor", required = true) BasePTupleRowProcessor processor,
            @JsonProperty(value = "sink", required = true) BasePTupleRowSink sink)
    {
        super(name);
        this.window = window;
        this.registry = registry;
        this.source = source;
        this.processor = processor;
        this.sink = sink;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void setup() {
        Lists.newArrayList(source, processor, sink).forEach(element -> {
            if (element.windowIsNeed()) element.setWindow(window);
            if (element.registryIsNeed()) element.setRegistry(registry);
            element.setup();
        });
    }

    @Override
    public void expand(Pipeline pipeline, PipelineOptions options) {
        PCollectionTuple flow =
                pipeline.apply(source).apply(processor);

        if(isDryRun(options)) {
            print100Row(flow);
            return;
        }

        flow.apply(sink);
    }

    private boolean isDryRun(PipelineOptions options) {
        return options.as(SpecOptions.class).getDryRun();
    }

    private void print100Row(PCollectionTuple flow) {
        for (TupleTag<Row> tag : PTupleUtils.<Row>materializeTags(flow.getAll().keySet())) {
            flow.get(tag).apply(Sample.any(100)).apply(MapElements.via(printRows));
        }
    }

    private static SimpleFunction<Row, Row> printRows = new SimpleFunction<Row, Row>() {
        @Override
        public Row apply(Row input) {
            log.info(input.toString());
            return input;
        }
    };
}