package com.plarium.south.superflow.core.spec.sink.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;

import javax.validation.constraints.NotNull;


@JsonPropertyOrder({"tag", "topic", "schema", "eventTime"})
public class TopicSinkMapping extends BaseSinkMapping {

    @Getter @NotNull
    @JsonPropertyDescription("The kafka topic name")
    private final String topic;


    @JsonCreator
    public TopicSinkMapping(
            @JsonProperty(value = "schema") String schema,
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "topic", required = true) String topic,
            @JsonProperty(value = "inferSchema") Boolean inferSchema,
            @JsonProperty(value = "namespace") String namespace)
    {
        super(tag, schema, eventTime, inferSchema, namespace);
        this.topic = topic;
    }
}
