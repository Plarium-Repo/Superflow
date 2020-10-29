package com.plarium.south.superflow.core.spec.processor.enrich;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.processor.BasePTupleRowProcessor;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import com.plarium.south.superflow.core.utils.BeamRowUtils;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import javax.validation.constraints.NotNull;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;

@SchemaDefinition(
        type = PROCESSOR,
        baseTypes = {"window", "enricher"},
        ignoreFields = {"registry"},
        required = {"type", "inputTag", "outputTag", "enricher"})

@JsonPropertyOrder({"type", "name", "registry", "window", "inputTag", "outputTag", "enricher"})
public class EnrichProcessor extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/enrich";

    @Getter @NotNull
    @JsonPropertyDescription("The name for origin dataset in input tuple")
    private final String inputTag;

    @Getter @NotNull
    @JsonPropertyDescription("The name for enriched dataset in output tuple")
    private final String outputTag;

    @Getter @NotNull
    @JsonPropertyDescription("The enricher")
    private final BaseEnricher enricher;


    @JsonCreator
    public EnrichProcessor(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "inputTag", required = true) String inputTag,
            @JsonProperty(value = "outputTag", required = true) String outputTag,
            @JsonProperty(value = "enricher", required = true) BaseEnricher enricher)
    {
        super(name, null, window);
        this.inputTag = inputTag;
        this.outputTag = outputTag;
        this.enricher = enricher;
    }

    @Override
    public boolean registryIsNeed() {
        return false;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        PCollection<Row> originDataset = getByTag(input, inputTag);
        Schema originSchema = originDataset.getSchema();
        Schema enricherSchema = enricher.querySchema();

        Schema mergeSchema = BeamRowUtils.mergeSchemas(originSchema, enricherSchema);
        PCollection<Row> enricherDataset = enricher.expand(mergeSchema, originDataset);

        return input.and(outputTag, applyWindow(enricherDataset));
    }
}