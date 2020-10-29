package com.plarium.south.superflow.core.spec.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

public class BaseMapping implements Serializable {

    @Getter @NotNull
    @JsonPropertyDescription("Pipeline tuple tag for access data (using in sql and output)")
    private final String tag;

    @Getter @Setter
    @JsonPropertyDescription("The event time field name in ms")
    private String eventTime;


    @JsonCreator
    public BaseMapping(
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "eventTime") String eventTime)
    {
        this.tag = tag;
        this.eventTime = eventTime;
    }

    public boolean hasEventTime() {
        return eventTime != null;
    }
}
