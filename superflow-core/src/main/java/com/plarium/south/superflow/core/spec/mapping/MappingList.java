package com.plarium.south.superflow.core.spec.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.SetupSpec;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.List;

@JsonPropertyOrder({"eventTime", "list"})
public class MappingList<T extends BaseMapping> implements Serializable, SetupSpec {

    @Getter @NotNull
    @JsonPropertyDescription("The default event time field name")
    private final String eventTime;

    @Getter @NotNull
    @JsonPropertyDescription("The list of mapping")
    protected final List<T> list;

    @JsonCreator
    public MappingList(
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "list", required = true) List<T> list)
    {
        this.list = list;
        this.eventTime = eventTime;
    }

    @Override
    public void setup() {
        list.forEach(m -> {
            if (!m.hasEventTime()) m.setEventTime(eventTime);
        });
    }
}
