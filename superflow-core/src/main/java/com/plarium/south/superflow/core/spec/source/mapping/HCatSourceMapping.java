package com.plarium.south.superflow.core.spec.source.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.util.List;

public class HCatSourceMapping extends BaseSourceMapping {

    @Getter @NotNull
    @JsonPropertyDescription("The table name to read from")
    private final String table;

    @Getter @NotNull
    @JsonPropertyDescription("The partition filter, for example: date=yyyy-mm-dd")
    private final String filter;

    @Getter @Setter
    @NotNull
    @JsonPropertyDescription("The database name. Set it if you want use some databases in source component")
    private String database;

    @Getter @NotNull
    @JsonPropertyDescription("The names of the columns that are partitions")
    private final List<String> partitions;


    @JsonCreator
    public HCatSourceMapping(
            @JsonProperty(value = "filter") String filter,
            @JsonProperty(value = "database") String database,
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "partitions") List<String> partitions,
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "table", required = true) String table)
    {
        super(tag, eventTime);
        this.table = table;
        this.filter = filter;
        this.database = database;
        this.partitions = partitions;
    }

    public boolean hasDatabase () {
        return database != null;
    }
}
