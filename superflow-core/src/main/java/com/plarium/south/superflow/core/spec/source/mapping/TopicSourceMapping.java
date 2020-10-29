package com.plarium.south.superflow.core.spec.source.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"tag", "schema", "eventTime", "topic", "autoCommit", "delayCommit"})
public class TopicSourceMapping extends BaseSourceMapping {

    @Getter @NotNull
    @JsonPropertyDescription("The kafka topic name")
    protected final String topic;

    @Getter @NotNull
    @JsonPropertyDescription("The schema name for create it in registry if needed")
    protected final String schema;

    @Getter @Setter
    @JsonPropertyDescription("The enable auto commit kafka offset, default: true")
    protected Boolean autoCommit;

    @Getter @Setter
    @JsonPropertyDescription("Delay between offset commits, default: '5 sec' (working if option autoCommit: true)")
    protected String delayCommit;

    @JsonCreator
    public TopicSourceMapping(
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "autoCommit") Boolean autoCommit,
            @JsonProperty(value = "delayCommit") String delayCommit,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "topic", required = true) String topic,
            @JsonProperty(value = "schema", required = true) String schema)
    {
        super(tag, eventTime);
        this.topic = topic;
        this.schema = schema;
        this.autoCommit = autoCommit;
        this.delayCommit = delayCommit;
    }

    public boolean hasSchema() {
        return schema != null;
    }
}
