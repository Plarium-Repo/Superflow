package com.plarium.south.superflow.core.spec.source.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;


@JsonPropertyOrder({"tag", "query", "eventTime"})
public class BQSourceMapping extends BaseSourceMapping {

    @Getter
    @JsonPropertyDescription("The input sql query")
    private final String query;

    @JsonCreator
    public BQSourceMapping(
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "query", required = true) String query)
    {
        super(tag, eventTime);
        this.query = query;
    }
}
