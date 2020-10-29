package com.plarium.south.superflow.core.spec.source.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.api.services.bigquery.model.TableRow;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.dofn.BQRowToBeamRowFn;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.source.mapping.BQSourceMapping;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.source.BasePTupleRowSource;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SOURCE;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority.INTERACTIVE;

@SchemaDefinition(
        type = SOURCE,
        baseTypes = {"registry", "window"},
        required = {"type", "mapping"})

@JsonPropertyOrder({"type", "name", "registry", "window", "mapping"})
public class BigQuerySource extends BasePTupleRowSource {
    public static final String TYPE = "source/gcp/bq";

    @Getter @NotNull
    @JsonPropertyDescription("The BigQuery mapping")
    private final MappingList<BQSourceMapping> mapping;



    @JsonCreator
    public BigQuerySource(@JsonProperty(value = "name") String name,
                          @JsonProperty(value = "registry") BaseRegistry registry,
                          @JsonProperty(value = "window") BaseWindow window,
                          @JsonProperty(value = "mapping", required = true) MappingList<BQSourceMapping> mapping)
    {
        super(name, registry, window);
        this.mapping = mapping;
    }

    @Override
    public void setup() {
        mapping.setup();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
        PCollectionTuple tuple = PCollectionTuple.empty(input.getPipeline());

        for (BQSourceMapping map : mapping.getList()) {
            PCollection<Row> dataset = expandPart(input, map);
            dataset = applyWindow(dataset);
            dataset = applyTimePolicy(dataset, map.getEventTime());
            tuple = tuple.and(map.getTag(), dataset);
        }

        return tuple;
    }

    private PCollection<Row> expandPart(PBegin input, BQSourceMapping map) {
        BigQueryIO.TypedRead<TableRow> reader = createReader(map);
        PCollection<TableRow> bqCollection = input.apply(reader);
        Schema bqSchema = bqCollection.getSchema();

        return bqCollection
                .apply(convertToBeamFormat(bqSchema))
                .setCoder(RowCoder.of(bqSchema));
    }

    private BigQueryIO.TypedRead<TableRow> createReader(BQSourceMapping map) {
        return BigQueryIO
                .readTableRowsWithSchema()
                .withoutResultFlattening()
                .withQueryPriority(INTERACTIVE)
                .fromQuery(map.getQuery())
                .usingStandardSql();
    }

    private ParDo.SingleOutput<TableRow, Row> convertToBeamFormat(Schema schema) {
        return ParDo.of(new BQRowToBeamRowFn(schema));
    }
}