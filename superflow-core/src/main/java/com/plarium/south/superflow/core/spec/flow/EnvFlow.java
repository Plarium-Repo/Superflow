package com.plarium.south.superflow.core.spec.flow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.options.SpecOptions;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.utils.ParseUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.FLOW;
import static com.plarium.south.superflow.core.utils.PathUtils.getPath;
import static com.plarium.south.superflow.core.utils.TemplateUtils.getOrDefaultForOsVariable;

@SchemaDefinition(
        type = FLOW,
        required = {"type", "env"})

@Slf4j
@JsonPropertyOrder({"type", "name", "env"})
public class EnvFlow extends RootElement {
    public static final String TYPE = "flow/env";

    @Getter
    private final Map<String, String> env;


    public EnvFlow(Map<String, String> env) {
        super(null);
        this.env = env;
    }

    public EnvFlow(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "env", required = true) Map<String, String> env)
    {
        super(name);
        this.env = replaceValuesFromOsEnv(env);
        log.info("Building environment variable is completed: {}", toJson());
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Puts new variable to the Environment
     * @param key variable key
     * @param value variable value
     *
     * @return current instance of the {@link #EnvFlow} for chaining
     */
    public EnvFlow put(String key, String value) {
        env.put(key, value);
        return this;
    }

    private static Map<String, String> replaceValuesFromOsEnv(Map<String, String> env) {
        for (String key : env.keySet()) {
            String  value = env.get(key);
            if (value.startsWith("${")) {
                env.put(key, getOrDefaultForOsVariable(value));
            }
        }

        return env;
    }

    public static EnvFlow fromOptions(SpecOptions options) throws IOException {
        if (options.getEnv() != null) {
            Path envPath = getPath(options.getEnv());
            return ParseUtils.parseYaml(envPath, EnvFlow.class);
        }
        else if (!options.getEnvMap().isEmpty()) {
            return new EnvFlow(options.getEnvMap());
        }

        return new EnvFlow(Maps.newHashMap());
    }

    private String toJson() {
        try {
            return new ObjectMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(env);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
