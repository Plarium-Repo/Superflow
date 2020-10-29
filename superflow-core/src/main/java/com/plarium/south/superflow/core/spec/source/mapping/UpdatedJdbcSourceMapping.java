package com.plarium.south.superflow.core.spec.source.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.utils.DurationUtils;
import lombok.Getter;
import lombok.Setter;
import org.joda.time.Duration;

import javax.validation.constraints.NotNull;


@JsonPropertyOrder({"tag", "query", "eventTime"})
public class UpdatedJdbcSourceMapping extends BaseSourceMapping {

    @Getter @NotNull
    @JsonPropertyDescription("The sql template for enrich query")
    private final String query;

    @Getter @Setter
    @JsonPropertyDescription("The time duration before each read from jdbc")
    private String duration;


    @JsonCreator
    public UpdatedJdbcSourceMapping(
            @JsonProperty(value = "duration") String duration,
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "query", required = true) String query)
    {
        super(tag, eventTime);
        this.query = query;
        this.duration = duration;
    }

    public boolean hasDuration() {
        return duration != null;
    }

    public Duration getDurationObject() {
        return DurationUtils.parse(duration);
    }
}
