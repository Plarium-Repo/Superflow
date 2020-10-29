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
import java.io.Serializable;
import java.util.List;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.JSONP;

@JsonPropertyOrder({"name", "ops"})
@SchemaDefinition(type = JSONP, required = "ops")
public class AddConstPathOperator extends JsonPathOperator {
    public static final String TYPE = "jsonp/ops/const";

    @Getter @NotNull
    @JsonPropertyDescription("The const operator list")
    private final List<Mapping> ops;


    @JsonCreator
    public AddConstPathOperator(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "ops", required = true) List<Mapping> ops)
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
        ops.forEach(m -> JsonPath.compile(m.path));
    }

    @Override
    public DocumentContext apply(DocumentContext doc, JsonPathPipeline context) {
        for (Mapping op : ops) {
            Object jsonValue = JsonPath.parse(op.val).read("$");
            doc = doc.put(op.path, op.key, jsonValue);
        }

        return doc;
    }


    @JsonPropertyOrder({"key", "path", "val"})
    public static class Mapping implements Serializable {

        @Getter @NotNull
        @JsonPropertyDescription("The key name for const value")
        private final String key;

        @Getter @NotNull
        @JsonPropertyDescription("The jsonpath where need add const")
        private final String path;

        @Getter @NotNull
        @JsonPropertyDescription("The const value, support any json struct")
        private final String val;


        @JsonCreator
        public Mapping(
                @JsonProperty(value = "key", required = true) String key,
                @JsonProperty(value = "path", required = true) String path,
                @JsonProperty(value = "val", required = true) String val)
        {
            this.key = key;
            this.path = path;
            this.val = val;
        }
    }
}
