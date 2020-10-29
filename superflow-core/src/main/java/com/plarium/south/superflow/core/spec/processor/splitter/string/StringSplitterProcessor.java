package com.plarium.south.superflow.core.spec.processor.splitter.string;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.processor.BasePTupleRowProcessor;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;
import static org.apache.beam.sdk.schemas.Schema.FieldType.*;


@SchemaDefinition(
        type = PROCESSOR,
        ignoreFields = {"registry"},
        required = {"type", "inputField", "outputField", "splitter", "inputTag", "outputTag"})

@JsonPropertyOrder({"type", "registry", "window", "inputField", "splitter", "outputField", "dropInput", "castElementTypeTo", "inputTag", "outputTag"})
public class StringSplitterProcessor extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/splitter/string";


    @Getter @NotNull
    @JsonPropertyDescription("The input field name for split")
    private final String inputField;

    @Getter @NotNull
    @JsonPropertyDescription("The output array field name")
    private final String outputField;

    @Getter @NotNull
    @JsonPropertyDescription("The splitter for input string")
    private final String splitter;

    @Getter
    @JsonPropertyDescription("The drop input field for split from row, default: false")
    private final Boolean dropInput;

    @Getter @NotNull
    @JsonPropertyDescription("The name for origin dataset in input tuple")
    private final String inputTag;

    @Getter @NotNull
    @JsonPropertyDescription("The name for dataset with splitted value in output tuple")
    private final String outputTag;

    @Getter
    @JsonPropertyDescription("The cast element type to numeric value, default: string")
    private final CastVariableType castElement;

    @JsonCreator
    protected StringSplitterProcessor(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "dropInput") Boolean dropInput,
            @JsonProperty(value = "castElement") CastVariableType castElement,
            @JsonProperty(value = "splitter", required = true) String splitter,
            @JsonProperty(value = "inputTag", required = true) String inputTag,
            @JsonProperty(value = "outputTag", required = true) String outputTag,
            @JsonProperty(value = "inputField", required = true) String inputField,
            @JsonProperty(value = "outputField", required = true) String outputField)
    {
        super(name, null, null);
        this.splitter = splitter;
        this.inputTag = inputTag;
        this.outputTag = outputTag;
        this.inputField = inputField;
        this.outputField = outputField;
        this.dropInput = firstNonNull(dropInput, false);
        this.castElement = firstNonNull(castElement, CastVariableType.STRING);
    }


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean registryIsNeed() {
        return false;
    }

    @Override
    public boolean windowIsNeed() {
        return false;
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        PCollection<Row> dataset = getByTag(input, inputTag);

        Schema inputSchema = dataset.getSchema();
        Schema outputSchema = getOutputSchema(dataset.getSchema());

        dataset = dataset.apply(splitterFn(inputSchema, outputSchema));
        dataset.setRowSchema(outputSchema);
        return input.and(outputTag, dataset);
    }

    private Schema getOutputSchema(Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>();
        List<Schema.Field> inputFields = inputSchema.getFields();

        for (Schema.Field field : inputFields) {
            if (!(Objects.equals(field.getName(), inputField)  && dropInput)) {
                outputFields.add(field);
            }
        }

        return Schema.builder()
                .addFields(outputFields)
                .addArrayField(outputField, elementType())
                .build();
    }

    private ParDo.SingleOutput<Row, Row> splitterFn(Schema inputSchema, Schema outputSchema) {
        return ParDo.of(new StringSplitterDoFn(inputField, splitter, dropInput, inputSchema, outputSchema, castElement));
    }

    private Schema.FieldType elementType() {
        switch (castElement) {
            case INT: return INT32;
            case LONG: return INT64;
            case DOUBLE: return DOUBLE;
            case STRING: return STRING;
            default:  throw new IllegalStateException("Unexpected value: " + castElement);
        }
    }
}
