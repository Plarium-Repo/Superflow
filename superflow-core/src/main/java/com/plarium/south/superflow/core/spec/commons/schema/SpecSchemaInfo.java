package com.plarium.south.superflow.core.spec.commons.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plarium.south.superflow.core.utils.ParseUtils;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

@ToString
public class SpecSchemaInfo {

    @Getter @NotNull
    private final String type;

    @Getter @NotNull
    private final SpecType specType;

    @Getter @NotNull
    private final JsonNode jsonSchema;

    @Getter @NotNull
    private final String[] required;

    @Getter
    private final String[] baseTypes;


    public SpecSchemaInfo(
            String type,
            SpecType specType,
            JsonNode jsonSchema,
            String[] required,
            @Nullable String[] baseTypes)
    {
        this.type = type;
        this.specType = specType;
        this.required = required;
        this.baseTypes = baseTypes;
        this.jsonSchema = jsonSchema;
    }


    public String yamlSchema(Boolean dropBaseTypes) {
        if (dropBaseTypes && baseTypes != null) {
            ObjectNode properties = (ObjectNode) jsonSchema.get("properties");
            for (String base : baseTypes) properties.remove(base);
        }

        return ParseUtils.jsonToYaml(jsonSchema.toString());
    }

    public String jsonSchema() {
        return jsonSchema.toString();
    }

    public static SpecSchemaInfo of(String type, JsonNode jsonSchema, SchemaDefinition def) {
        return new SpecSchemaInfo(type, def.type(), jsonSchema, def.required(), def.baseTypes());
    }
}
