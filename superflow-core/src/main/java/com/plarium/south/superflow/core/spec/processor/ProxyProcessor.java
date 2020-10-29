package com.plarium.south.superflow.core.spec.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.beam.sdk.values.PCollectionTuple;

@JsonPropertyOrder({"type", "name"})
public class ProxyProcessor extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/proxy";

    public ProxyProcessor(
            @JsonProperty(value = "name") String name)
    {
        super(name, null, null);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        return input;
    }
}
