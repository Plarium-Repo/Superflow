package com.plarium.south.superflow.core.spec.commons.jsonpath.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.plarium.south.superflow.core.spec.commons.jsonpath.JsonPathPipeline;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.JSONP;

@JsonPropertyOrder({"name", "select"})
@SchemaDefinition(type = JSONP, required = "select")
public class SelectJsonPathOperator extends JsonPathOperator {
    public static final String TYPE = "jsonp/ops/select";

    @Getter
    @JsonPropertyDescription("The jsonpath for select operation")
    private final String select;

    @JsonCreator
    public SelectJsonPathOperator(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "select", required = true) String select)
    {
        super(name);
        this.select = select;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() {
        JsonPath.compile(select);
    }

    @Override
    public DocumentContext apply(DocumentContext doc, JsonPathPipeline context) throws JsonProcessingException {
        Object value = doc.read(select);
        String jsonValue = context.getMapper().writeValueAsString(value);
        return JsonPath.parse(jsonValue);
    }
}
