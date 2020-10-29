package com.plarium.south.superflow.core.spec.commons.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonPropertyOrder({"name", "order", "type"})
public class StatementMapping implements Serializable {

    @Getter @NotNull
    @JsonPropertyDescription("The variable name from origin row")
    private final String name;

    @Getter @NotNull
    @JsonPropertyDescription("The order for variable")
    private final Integer order;

    @Getter @NotNull
    @JsonPropertyDescription("One of from: short, int, long, decimal, float, double, string, boolean")
    private final VariableType type;


    @JsonCreator
    public StatementMapping(
            @JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "order", required = true) Integer order,
            @JsonProperty(value = "type", required = true) VariableType type)
    {
        this.order = order;
        this.name = name;
        this.type = type;
    }

    public enum VariableType {
        SHORT("short"), INT("int"), LONG("long"), DECIMAL("decimal"),
        FLOAT("float"), DOUBLE ("double"), STRING("string"), BOOL("boolean");

        private static Map<String, VariableType> FORMATS = Stream.of(VariableType.values())
                .collect(Collectors.toMap(s -> s.type, Function.identity()));

        private final String type;

        VariableType(String type) {
            this.type = type;
        }

        @JsonCreator
        public static VariableType forVal(String val) {
            return FORMATS.get(val);
        }
    }
}
