package com.plarium.south.superflow.core.spec.sink.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.api.services.bigquery.model.TableRow;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.dofn.BeamRowToBQRowFn;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.BasePTupleRowSink;
import com.plarium.south.superflow.core.spec.sink.mapping.BQSinkMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SINK;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.*;


@SchemaDefinition(
        type = SINK,
        ignoreFields = {"window", "registry"},
        required = {"type", "mapping"})

@JsonPropertyOrder({"type", "name", "registry", "window", "mapping"})
public class BigQuerySink extends BasePTupleRowSink {
    public static final String TYPE = "sink/gcp/bq";

    @Getter
    @JsonPropertyDescription("The BigQuery mapping")
    private final MappingList<BQSinkMapping> mapping;

    @JsonCreator
    public BigQuerySink(@JsonProperty(value = "name") String name,
                        @JsonProperty(value = "registry") BaseRegistry registry,
                        @JsonProperty(value = "window") BaseWindow window,
                        @JsonProperty(value = "mapping") MappingList<BQSinkMapping> mapping)
    {
        super(name, registry, false, window);
        this.mapping = mapping;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void setup() {
        mapping.setup();
    }

    @Override
    public PDone expand(PCollectionTuple input) {

        for (BQSinkMapping map : mapping.getList()) {
            PCollection<Row> dataset = input.get(map.getTag());
            dataset = applyWindow(dataset);
            dataset = applyTimePolicy(dataset, map.getEventTime());
            writePart(dataset, map);
        }

        return PDone.in(input.getPipeline());
    }

    protected void writePart(PCollection<Row> input, BQSinkMapping map) {
        Schema beamSchema = input.getSchema();
        String schemaText = inferOrFetchSchema(map, input);
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaText);

        input.apply(ParDo.of(new BeamRowToBQRowFn()))
                .setRowSchema(beamSchema)
                .apply(createWriter(map));

        createSchemaIfNeeded(map.getSchema(), avroSchema.toString());
    }

    private BigQueryIO.Write<TableRow> createWriter(BQSinkMapping map) {

        return BigQueryIO.writeTableRows()
                .withWriteDisposition(map.writeDisposition())
                .withCreateDisposition(createMode(map))
                .to(map.getTable())
                .useBeamSchema();
    }

    private BigQueryIO.Write.CreateDisposition createMode(BQSinkMapping map) {
        Boolean allowCreateTable = map.getAllowCreate();
        return allowCreateTable ? CREATE_IF_NEEDED : CREATE_NEVER;
    }
}
