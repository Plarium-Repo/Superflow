package com.plarium.south.superflow.core.spec.sink.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.sink.bigquery.BQWriteMode;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

import static com.google.common.base.MoreObjects.firstNonNull;


@JsonPropertyOrder({"tag", "table", "allowCreate", "writeMode", "schema", "eventTime"})
public class BQSinkMapping extends BaseSinkMapping {

    @Getter
    @JsonPropertyDescription("The BigQuery table to write, format: [project.]dataset.table")
    private final String table;

    @Getter
    @JsonPropertyDescription("BigQuery write mode: [append, truncate, empty], default: empty")
    private final BQWriteMode writeMode;

    @Getter
    @JsonPropertyDescription("Allow create new  tables in BigQuery")
    private final Boolean allowCreate;


    @JsonCreator
    public BQSinkMapping(
            @JsonProperty(value = "tag", required = true) String tag,
            @JsonProperty(value = "table", required = true) String table,
            @JsonProperty(value = "writeMode") BQWriteMode writeMode,
            @JsonProperty(value = "allowCreate") Boolean allowCreate,
            @JsonProperty(value = "eventTime") String eventTime,
            @JsonProperty(value = "schema") String schema,
            @JsonProperty(value = "inferSchema") Boolean inferSchema,
            @JsonProperty(value = "namespace") String namespace)
    {
        super(tag, schema, eventTime, inferSchema, namespace);
        this.table = table;
        this.allowCreate = allowCreate;
        this.writeMode = firstNonNull(writeMode, BQWriteMode.empty);
    }

    public WriteDisposition writeDisposition() {
        switch (writeMode) {
            case append:
                return WriteDisposition.WRITE_APPEND;
            case truncate:
                return WriteDisposition.WRITE_TRUNCATE;
            case empty:
                return WriteDisposition.WRITE_EMPTY;
            default:
                throw new IllegalStateException("Unexpected value: " + writeMode);
        }
    }
}
