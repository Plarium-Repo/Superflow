package com.plarium.south.superflow.core.spec.source.updated.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.source.mapping.UpdatedJdbcSourceMapping;
import lombok.Getter;

import java.util.List;

@JsonPropertyOrder({"timeout", "eventTime", "list"})
public class UpdatedJdbcSourceMappingList extends MappingList<UpdatedJdbcSourceMapping> {

    @Getter
    @JsonPropertyDescription("The time duration before each read from jdbc, default for list elements")
    private final String duration;


    @JsonCreator
    public UpdatedJdbcSourceMappingList(
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "duration", required = true) String duration,
            @JsonProperty(value = "list", required = true) List<UpdatedJdbcSourceMapping> list)
    {
        super(eventTime, list);
        this.duration = duration;
    }

    @Override
    public void setup() {
        super.setup();

        list.forEach(m -> {
            if (!m.hasDuration()) m.setDuration(duration);
        });
    }
}
