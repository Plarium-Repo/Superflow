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
public class RenamePathOperator extends JsonPathOperator {
    public static final String TYPE = "jsonp/ops/rename";

    @Getter
    @JsonPropertyDescription("The const operator list")
    private final List<Mapping> ops;


    @JsonCreator
    public RenamePathOperator(
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
            doc = doc.renameKey(op.path, op.old, op.newName);
        }

        return doc;
    }


    public static class Mapping implements Serializable {

        @Getter @NotNull
        @JsonPropertyDescription("The jsonpath to rename property")
        private final String path;

        @Getter @NotNull
        @JsonPropertyDescription("The old  name for property")
        private final String old;

        @Getter @NotNull
        @JsonPropertyDescription("The new name for property")
        private final String newName;


        @JsonCreator
        public Mapping(
                @JsonProperty(value = "path", required = true) String path,
                @JsonProperty(value = "old", required = true) String old,
                @JsonProperty(value = "new", required = true) String new_)
        {
            this.path = path;
            this.old = old;
            this.newName = new_;
        }
    }
}
