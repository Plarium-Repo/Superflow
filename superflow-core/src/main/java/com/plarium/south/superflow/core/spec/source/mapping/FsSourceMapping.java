package com.plarium.south.superflow.core.spec.source.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;


@JsonPropertyOrder({"tag", "path", "schema", "eventTime"})
public class FsSourceMapping extends BaseSourceMapping {

    @Getter
    @JsonPropertyDescription("The directory with data, example: /path/to/data/**")
    private final String path;

    @Getter
    @JsonPropertyDescription("The schema name for create it in registry if needed")
    private final String schema;


    @JsonCreator
    public FsSourceMapping(
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "schema", required = true) String schema)
    {
        super(tag, eventTime);
        this.path = path;
        this.schema = schema;
    }
}
