package com.plarium.south.superflow.core.spec.processor.jsonp;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.jayway.jsonpath.Option;
import com.plarium.south.superflow.core.spec.commons.jsonpath.operator.JsonPathOperator;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.processor.BasePTupleRowProcessor;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;

@SchemaDefinition(
        type = PROCESSOR,
        baseTypes = {"window", "registry"},
        required = {"type", "inputTag", "outputTag", "schema", })

@JsonPropertyOrder({"type", "name", "registry", "window", "inputTag",
        "outputTag", "schema", "avroSchema", "select", "ignoreEmpty", "options", "pipeline"})
public class JsonPathProcessor extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/jsonp";


    @Getter @NotNull
    @JsonPropertyDescription("The name for origin dataset in input tuple")
    private final String inputTag;

    @Getter @NotNull
    @JsonPropertyDescription("The name for dataset with jsonp processed values")
    private final String outputTag;

    @Getter
    @JsonPropertyDescription("The output schema name")
    private final String outputSchema;

    @Getter
    @JsonPropertyDescription("The output specific avro schema")
    private final String outputAvroSchema;

    @Getter
    @JsonPropertyDescription("The jsonpath options, " +
            "see more https://javadoc.io/doc/com.jayway.jsonpath/json-path/2.4.0/com/jayway/jsonpath/Option.html")
    private final Set<Option> options;

    @Getter
    @JsonPropertyDescription("The first select jsonpath expression for pipeline, default: $")
    private final String select;

    @Getter
    @JsonPropertyDescription("Allow ignore empty object from json pipeline output, default: true")
    private final Boolean ignoreEmpty;

    @Getter
    @JsonPropertyDescription("The list of jsonpath operators")
    private final List<JsonPathOperator> pipeline;


    @JsonCreator
    public JsonPathProcessor(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "outputSchema") String outputSchema,
            @JsonProperty(value = "outputAvroSchema") String outputAvroSchema,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "select") String select,
            @JsonProperty(value = "options") Set<Option> options,
            @JsonProperty(value = "inputTag", required = true) String inputTag,
            @JsonProperty(value = "outputTag", required = true) String outputTag,
            @JsonProperty(value = "ignoreEmpty") Boolean ignoreEmpty,
            @JsonProperty(value = "pipeline") List<JsonPathOperator> pipeline)
    {
        super(name, registry, null);
        this.outputSchema = outputSchema;
        this.inputTag = inputTag;
        this.outputTag = outputTag;
        this.options = options;
        this.select = select;
        this.pipeline = pipeline;
        this.outputAvroSchema = outputAvroSchema;
        this.ignoreEmpty = firstNonNull(ignoreEmpty, true);

        checkState(
                (outputSchema == null && outputAvroSchema != null) || (outputSchema != null && outputAvroSchema == null),
                "The only one property 'outputSchema' or 'outputAvroSchema' is required for construct mapping"
        );

        checkState(
                select != null ||  pipeline != null,
                "The property 'select' or/and 'pipeline' is required for construct mapping"
        );
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean windowIsNeed() {
        return false;
    }

    @Override
    public void setup() {
        pipeline.forEach(JsonPathOperator::validate);
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        PCollection<Row> dataset = getByTag(input, inputTag);

        Schema inputSchema = dataset.getSchema();
        Schema outputSchema = fetchOrGetSchema();
        dataset = dataset.apply(jsonPathParDoFn(inputSchema, outputSchema));
        dataset.setRowSchema(outputSchema);

        return input.and(outputTag, dataset);
    }

    private Schema fetchOrGetSchema() {
        org.apache.avro.Schema schema;
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

        if (outputSchema != null) {
            String schemaText = registry.getSchema(outputSchema);
            schema = parser.parse(schemaText);
        }
        else {
            schema = parser.parse(outputAvroSchema);
        }

        return AvroUtils.toBeamSchema(schema);
    }

    private ParDo.SingleOutput<Row, Row> jsonPathParDoFn(Schema inputSchema, Schema outputSchema) {
        return ParDo.of(new JsonPathStringDoFn(select, inputSchema, outputSchema, options,  pipeline, ignoreEmpty));
    }
}
