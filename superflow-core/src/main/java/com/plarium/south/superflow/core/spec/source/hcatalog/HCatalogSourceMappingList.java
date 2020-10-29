package com.plarium.south.superflow.core.spec.source.hcatalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.source.mapping.HCatSourceMapping;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import java.util.List;

public class HCatalogSourceMappingList extends MappingList<HCatSourceMapping> {

    @Getter @NotNull
    @JsonPropertyDescription("The default database name for all list elements")
    private final String database;


    @JsonCreator
    public HCatalogSourceMappingList(
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "database", required = true) String database,
            @JsonProperty(value = "list", required = true) List<HCatSourceMapping> list)
    {
        super(eventTime, list);
        this.database = database;
    }

    @Override
    public void setup() {
        super.setup();

        list.forEach(m -> {
            if (!m.hasDatabase()) m.setDatabase(database);
        });
    }
}
