package com.plarium.south.superflow.core.spec.processor;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.plarium.south.superflow.core.spec.processor.enrich.EnrichProcessor;
import com.plarium.south.superflow.core.spec.processor.jsonp.JsonPathProcessor;
import com.plarium.south.superflow.core.spec.processor.splitter.string.StringSplitterProcessor;
import com.plarium.south.superflow.core.spec.processor.sql.SqlProcessor;
import com.plarium.south.superflow.core.spec.processor.transform.avro.AvroEnumTransform;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = SqlProcessor.TYPE, value = SqlProcessor.class),
        @JsonSubTypes.Type(name = ProxyProcessor.TYPE, value = ProxyProcessor.class),
        @JsonSubTypes.Type(name = EnrichProcessor.TYPE, value = EnrichProcessor.class),
        @JsonSubTypes.Type(name = JsonPathProcessor.TYPE, value = JsonPathProcessor.class),
        @JsonSubTypes.Type(name = CompositeProcessor.TYPE, value = CompositeProcessor.class),
        @JsonSubTypes.Type(name = StringSplitterProcessor.TYPE, value = StringSplitterProcessor.class),
        @JsonSubTypes.Type(name = AvroEnumTransform.TYPE, value = AvroEnumTransform.class)
})
@JsonPropertyOrder({"name", "window", "registry"})
public abstract class BasePTupleRowProcessor extends BaseProcessor<Row, PCollectionTuple, PCollectionTuple> {

    protected BasePTupleRowProcessor(
            @Nullable String name,
            @Nullable BaseRegistry registry,
            @Nullable BaseWindow window)
    {
        super(name, registry, window);
    }
}
