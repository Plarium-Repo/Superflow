package com.plarium.south.superflow.core.spec.processor.splitter.string;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum CastVariableType {
    INT("int"), LONG("long"), DOUBLE("double"), STRING("string");

    private static Map<String, CastVariableType> FORMATS = Stream.of(CastVariableType.values())
            .collect(Collectors.toMap(s -> s.type, Function.identity()));

    private final String type;

    CastVariableType(String type) {
        this.type = type;
    }

    @JsonCreator
    public static CastVariableType forVal(String val) {
        return FORMATS.get(val);
    }
}
