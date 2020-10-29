package com.plarium.south.superflow.core.spec.sink.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;

import javax.validation.constraints.NotNull;


@JsonPropertyOrder({"tag", "path", "schema", "eventTime"})
public class FsSinkMapping extends BaseSinkMapping {

    @Getter @NotNull
    @JsonPropertyDescription("The directory with data, example: /path/to/data/**")
    private final String path;


    @JsonCreator
    public FsSinkMapping(
            @JsonProperty(value = "schema") String schema,
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "inferSchema") Boolean inferSchema,
            @JsonProperty(value = "namespace") String namespace)
    {
        super(tag, schema, eventTime, inferSchema, namespace);
        this.path = path;
    }
}
