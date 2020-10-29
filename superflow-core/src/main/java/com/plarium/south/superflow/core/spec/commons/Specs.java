package com.plarium.south.superflow.core.spec.commons;

import com.fasterxml.jackson.core.type.TypeReference;
import com.plarium.south.superflow.core.utils.ParseUtils;
import com.plarium.south.superflow.core.spec.flow.EnvFlow;
import com.plarium.south.superflow.core.spec.flow.RootElement;
import com.plarium.south.superflow.core.utils.PathUtils;
import com.plarium.south.superflow.core.utils.TemplateUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class Specs {
    private static final TypeReference<RootElement> PARSE_TYPE = new TypeReference<RootElement>() {};

    private final List<RootElement> specs;


    public Specs(EnvFlow env, Path specPath) throws IOException {
        String yamlTemplate = PathUtils.getContentAsString(specPath);
        String yamlRendered = TemplateUtils.renderYamlSpec(yamlTemplate, env.getEnv());
        log.info("Building superflow spec is completed:\n {}", yamlRendered);
        this.specs = ParseUtils.parseYaml(yamlRendered.getBytes(), PARSE_TYPE);
    }

    public <T> T getSpecByType(Class<T> clazz) {
        return specs.stream()
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .findFirst()
                .orElseThrow(() -> new SpecNotFoundException(clazz));
    }

    public <T> List<T> getSpecsByType(Class<T> clazz) {
        return specs.stream()
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .collect(Collectors.toList());
    }

    public static Specs of(EnvFlow env, Path specPath) throws IOException {
        return new Specs(env, specPath);
    }

    private static class SpecNotFoundException extends RuntimeException {
        public SpecNotFoundException(Class<?> clazz) {
            super("Not found spec with type: " + clazz);
        }
    }
}
