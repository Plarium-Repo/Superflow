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
public class CopyJsonPathOperator extends JsonPathOperator {
    public static final String TYPE = "jsonp/ops/copy";

    @Getter @NotNull
    @JsonPropertyDescription("The copy operator list")
    private final List<Mapping> ops;

    @JsonCreator
    public CopyJsonPathOperator(
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
        ops.forEach(m -> {
            JsonPath.compile(m.from);
            JsonPath.compile(m.to);
        });
    }

    @Override
    public DocumentContext apply(DocumentContext doc, JsonPathPipeline context) {
        for (Mapping map : ops) {
            Object value = doc.read(map.from);
            doc = doc.put(map.to, map.key, value);
        }

        return doc;
    }


    @JsonPropertyOrder({"key", "from", "to"})
    public static class Mapping implements Serializable {

        @Getter @NotNull
        @JsonPropertyDescription("The key name for copy value")
        private final String key;

        @Getter @NotNull
        @JsonPropertyDescription("The jsonpath where contains value for copy")
        private final String from;

        @Getter @NotNull
        @JsonPropertyDescription("The key name for copy value")
        private final String to;


        @JsonCreator
        public Mapping(
                @JsonProperty(value = "key", required = true) String key,
                @JsonProperty(value = "from", required = true) String from,
                @JsonProperty(value = "to", required = true) String to)
        {
            this.key = key;
            this.from = from;
            this.to = to;
        }
    }
}
