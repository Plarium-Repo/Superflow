package com.plarium.south.superflow.core.spec.processor.transform.avro;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.commons.transform.EnumFieldMapping;
import com.plarium.south.superflow.core.spec.processor.BasePTupleRowProcessor;
import com.plarium.south.superflow.core.utils.AvroUtilsInternal;
import lombok.Getter;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;

@SchemaDefinition(
        type = PROCESSOR,
        ignoreFields = {"window", "registry"},
        required = {"type", "inputTag", "outputTag", "mappings"})
@JsonPropertyOrder({"type", "name", "registry", "window", "inputTag", "outputTag", "mappings"})
public class AvroEnumTransform extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/transform/avro/enum";

    @Getter
    @JsonPropertyDescription("The source input to transform")
    private final String inputTag;

    @Getter
    @JsonPropertyDescription("The transform output")
    private final String outputTag;

    @Getter
    @JsonPropertyDescription("The list of the field to transform")
    private final List<EnumFieldMapping> mappings;

    @Getter
    private final Map<String, AvroUtilsInternal.EnumConversionStrategy> columnMappings;

    public AvroEnumTransform(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "inputTag", required = true) String inputTag,
            @JsonProperty(value = "outputTag", required = true) String outputTag,
            @JsonProperty(value = "mappings", required = true) List<EnumFieldMapping> mappings) {
        super(name, null, null);

        this.inputTag = inputTag;
        this.outputTag = outputTag;
        this.mappings = mappings;

        this.columnMappings = createColumnsMap();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        PCollection<Row> inputCollection = getByTag(input, inputTag);
        Schema originSchema = inputCollection.getSchema();
        Schema outputSchema = AvroUtilsInternal.convertBeamSchema(originSchema,
                new AvroUtilsInternal.FieldConversion(this.columnMappings));

        return input.and(outputTag,
                inputCollection
                        .apply(ParDo.of(new SchemaTransformFn(originSchema, outputSchema,
                                new AvroUtilsInternal.FieldConversion(this.columnMappings))))
                        .setCoder(RowCoder.of(outputSchema)));
    }

    private final Map<String, AvroUtilsInternal.EnumConversionStrategy> createColumnsMap(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> resultMap = new HashMap<>();
        if (this.mappings == null){
            return resultMap;
        }

        for (EnumFieldMapping fieldMap : this.mappings) {
            String fieldPath = fieldMap.path;
            int wildcardPosition = fieldPath.lastIndexOf(".*");
            if (wildcardPosition > -1){ // Wildcard
                fieldPath = fieldPath.substring(0, wildcardPosition);

            }

            resultMap.put(fieldPath, fieldMap.conversionStrategy);
        }

        return resultMap;
    }
}
