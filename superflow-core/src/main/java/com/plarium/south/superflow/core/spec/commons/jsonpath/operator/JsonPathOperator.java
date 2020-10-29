package com.plarium.south.superflow.core.spec.commons.jsonpath.operator;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.jayway.jsonpath.DocumentContext;
import com.plarium.south.superflow.core.spec.commons.jsonpath.JsonPathPipeline;
import lombok.Getter;

import java.io.IOException;
import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")

@JsonSubTypes({
        @JsonSubTypes.Type(name = RenamePathOperator.TYPE, value = RenamePathOperator.class),
        @JsonSubTypes.Type(name = CopyJsonPathOperator.TYPE, value = CopyJsonPathOperator.class),
        @JsonSubTypes.Type(name = AddConstPathOperator.TYPE, value = AddConstPathOperator.class),
        @JsonSubTypes.Type(name = DeleteJsonPathOperator.TYPE, value = DeleteJsonPathOperator.class),
        @JsonSubTypes.Type(name = SelectJsonPathOperator.TYPE, value = SelectJsonPathOperator.class)
})
@JsonPropertyOrder({"name"})
public abstract class JsonPathOperator implements Serializable {

    @Getter
    private final String name;


    public JsonPathOperator(String name) {
        this.name = name;
    }

    public abstract String getType();

    public abstract void validate();

    public abstract DocumentContext apply(DocumentContext doc, JsonPathPipeline context) throws IOException;

}