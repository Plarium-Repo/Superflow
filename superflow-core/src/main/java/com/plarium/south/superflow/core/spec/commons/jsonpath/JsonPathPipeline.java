package com.plarium.south.superflow.core.spec.commons.jsonpath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.plarium.south.superflow.core.spec.commons.jsonpath.operator.JsonPathOperator;
import lombok.Getter;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;

public class JsonPathPipeline {
    public static final String DEFAULT_SELECT_PATH = "$";

    @Getter
    protected final ObjectMapper mapper;

    @Getter
    protected final Configuration  config;


    private final String select;

    private final List<JsonPathOperator> ops;


    private JsonPathPipeline(
            String select,
            ObjectMapper mapper,
            Configuration config,
            List<JsonPathOperator> ops)
    {
        this.ops = ops;
        this.mapper = mapper;
        this.config = config;
        this.select = firstNonNull(select, DEFAULT_SELECT_PATH);
    }

    public DocumentContext process(String json) throws IOException {
        if (!DEFAULT_SELECT_PATH.equals(select)) {
            json = applySelect(json);
        }

        DocumentContext doc = JsonPath.parse(json, config);

        if (ops == null) return doc;


        for (JsonPathOperator op : ops) {
            doc = op.apply(doc, this);
        }

        return doc;
    }

    private String applySelect(String json) throws JsonProcessingException {
        return objToJson(JsonPath
                .parse(json, config)
                .read(select));
    }

    private String objToJson(Object obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }


    public static class Builder {

        private String select;

        private ObjectMapper mapper;

        private Set<Option> options;

        private List<JsonPathOperator> operators;


        public Builder() {}


        public Builder select(String select) {
            this.select = select;
            return this;
        }

        public Builder mapper(ObjectMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public Builder options(Set<Option> options) {
            this.options = options;
            return this;
        }

        public Builder operators(List<JsonPathOperator> operators) {
            this.operators = operators;
            return this;
        }

        public JsonPathPipeline build() {
            Preconditions.checkState(
                    (operators != null && !operators.isEmpty()) || !DEFAULT_SELECT_PATH.equals(select),
                    "The minimum required field it 'operators' or 'select' != $");

            return new JsonPathPipeline(
                    firstNonNull(select, DEFAULT_SELECT_PATH),
                    firstNonNull(mapper, new ObjectMapper()),
                    buildConfig(),
                    operators);
        }

        private Configuration buildConfig() {
            Configuration config;

            if (options == null) {
                config = Configuration.defaultConfiguration();
            }
            else {
                config = Configuration
                        .builder()
                        .options(options)
                        .build();
            }

            return config;
        }

    }
}
