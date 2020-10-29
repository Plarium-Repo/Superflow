package com.plarium.south.superflow.core.spec.source.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import lombok.Getter;

import static com.google.common.base.Preconditions.checkState;

@JsonPropertyOrder({"tag", "schema", "eventTime", "topic", "autoCommit", "delayCommit", "avroSchema"})
public class TopicSourceAvroMapping extends TopicSourceMapping {

    @Getter
    @JsonPropertyDescription("Allow to set specific Avro schema for the incoming data")
    private final String avroSchema;

    @JsonCreator
    public TopicSourceAvroMapping(
            @JsonProperty(value = "schema") String schema,
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "avroSchema") String avroSchema,
            @JsonProperty(value = "autoCommit") Boolean autoCommit,
            @JsonProperty(value = "delayCommit") String delayCommit,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "topic", required = true) String topic)
    {
        super(eventTime, autoCommit, delayCommit, tag, topic, schema);
        this.avroSchema = avroSchema;

        checkState(
                (schema == null && avroSchema != null) || (schema != null && avroSchema == null),
            "The only one property 'schema' or 'avroSchema' is required for construct mapping"
        );
    }

    public String fetchOrGetAvroSchema(BaseRegistry registry) {
        if (hasSchema()) return registry.getSchema(schema);
        return avroSchema;
    }

    public boolean autoCommitIsNeeded() {
        return autoCommit == null;
    }

    public boolean delayCommitIsNeeded() {
        return delayCommit == null;
    }
}
