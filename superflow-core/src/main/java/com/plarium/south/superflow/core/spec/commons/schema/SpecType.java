package com.plarium.south.superflow.core.spec.commons.schema;

import lombok.Getter;

public enum  SpecType {

    FLOW("flow"),

    SOURCE("source"),

    PROCESSOR("processor"),

    SINK("sink"),

    REGISTRY("registry"),

    WINDOW("window"),

    TRIGGER("trigger"),

    ENRICHER("enricher"),

    JSONP("jsonp");


    @Getter
    private final String type;

    SpecType(String type) {
        this.type = type;
    }
}
