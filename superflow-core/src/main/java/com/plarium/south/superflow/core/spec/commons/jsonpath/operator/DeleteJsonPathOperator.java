package com.plarium.south.superflow.core.spec.commons.jsonpath.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.plarium.south.superflow.core.spec.commons.jsonpath.JsonPathPipeline;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.JSONP;

@JsonPropertyOrder({"name", "ops"})
@SchemaDefinition(type = JSONP, required = "ops")
public class DeleteJsonPathOperator extends JsonPathOperator {
    public static final String TYPE = "jsonp/ops/del";

    @Getter @NotNull
    @JsonPropertyDescription("The delete operator list")
    private final List<String> ops;


    @JsonCreator
    public DeleteJsonPathOperator(
            @JsonProperty(value = "name")String name,
            @JsonProperty(value = "ops", required = true) List<String> ops)
    {
        super(name);
        this.ops = ops;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() {
        ops.forEach(JsonPath::compile);
    }

    @Override
    public DocumentContext apply(DocumentContext doc, JsonPathPipeline context) {
        ops.forEach(doc::delete);
        return doc;
    }
}
