package com.plarium.south.superflow.core.spec.commons.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.utils.AvroUtilsInternal;
import lombok.Getter;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"path", "conversionStrategy"})
public class EnumFieldMapping {

    @Getter
    @NotNull
    @JsonPropertyDescription("The path to the field in source schema to convert. Supports wild cards")
    public String path;

    @Getter
    @NotNull
    @JsonPropertyDescription("The conversion strategy. Allowed values: \"to_int\" and \"to_string\"")
    public AvroUtilsInternal.EnumConversionStrategy conversionStrategy;

    @JsonCreator
    public EnumFieldMapping(
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "conversionStrategy", required = true) AvroUtilsInternal.EnumConversionStrategy conversionStrategy)
    {
        this.path = path;
        this.conversionStrategy = conversionStrategy;
    }
}
