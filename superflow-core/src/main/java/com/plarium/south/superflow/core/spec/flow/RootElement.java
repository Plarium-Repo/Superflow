package com.plarium.south.superflow.core.spec.flow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.plarium.south.superflow.core.spec.commons.SetupSpec;

import javax.annotation.Nullable;
import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = EnvFlow.TYPE, value = EnvFlow.class),
        @JsonSubTypes.Type(name = FlowOptions.TYPE, value = FlowOptions.class),
        @JsonSubTypes.Type(name = PTupleRowEtl.TYPE, value = PTupleRowEtl.class)
})
public abstract class RootElement implements Serializable, SetupSpec {

    private final String name;

    public RootElement(@Nullable String name) {
        this.name = name;
    }

    public abstract String getType();

    public String getName() {
        return name;
    }

    @Override
    public void setup() {}

}
