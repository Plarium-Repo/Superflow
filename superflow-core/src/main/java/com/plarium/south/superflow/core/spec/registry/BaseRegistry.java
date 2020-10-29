package com.plarium.south.superflow.core.spec.registry;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = HortonRegistry.TYPE, value = HortonRegistry.class)
})
@JsonPropertyOrder({"type", "url"})
public abstract class BaseRegistry implements Serializable {

    @Getter @NotNull
    protected final String url;

    public BaseRegistry(String url) {
        this.url = url;
    }

    public abstract String getType();

    public abstract Map<String, Object> serdeConfig();

    public abstract boolean hasSchema(String schemaName);

    public abstract String getSchema(String schemaName);

    public abstract Map<Integer, String> getAllVersion(String schemaName);

    public abstract void createSchema(String schemaName, String schema);
}
