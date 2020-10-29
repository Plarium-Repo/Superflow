package com.plarium.south.superflow.core.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.fasterxml.jackson.module.jsonSchema.factories.VisitorContext;
import com.fasterxml.jackson.module.jsonSchema.factories.WrapperFactory;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.commons.schema.SpecSchemaInfo;
import com.plarium.south.superflow.core.spec.commons.schema.SpecType;
import org.reflections.Reflections;
import org.springframework.util.Assert;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.plarium.south.superflow.core.utils.JsonPathUtils.removeRecursively;
import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;

public class SpecSchemaUtils {

    private static final Reflections REFLECTIONS;

    private static final Map<String, Class<?>> TYPE_TO_CLASS;

    private static final String PACKAGE = "com.plarium.south.superflow.core.spec";


    private SpecSchemaUtils() {}


    static {
        REFLECTIONS = new Reflections(PACKAGE);

        TYPE_TO_CLASS = REFLECTIONS
                .getTypesAnnotatedWith(SchemaDefinition.class)
                .stream()
                .collect(Collectors.toMap(
                        SpecSchemaUtils::extractValue,
                        clazz -> clazz));
    }

    private static String extractValue(Class<?> clazz) {
        Assert.notNull(clazz, "The clazz is required");

        Field field = findField(clazz, "TYPE");
        field.setAccessible(true);
        return getField(field, clazz).toString();
    }


    public static List<String> getSubtypes(SpecType type) {
        Assert.notNull(type, "The spec type is required");

        return TYPE_TO_CLASS.keySet()
                .stream()
                .filter(k -> k.startsWith(type.getType()))
                .collect(Collectors.toList());
    }

    public static SpecSchemaInfo getSchemaInfo(String type) {
        Class<?> clazz = TYPE_TO_CLASS.get(type);
        Assert.notNull(clazz, "Not found spec class by type: " + type);

        JsonNode jsonSchema = getJsonSchema(clazz);
        SchemaDefinition metadata = getDefinition(clazz);
        return SpecSchemaInfo.of(type, jsonSchema, metadata);
    }

    private static JsonNode getJsonSchema(Class<?> clazz) {
        Assert.notNull(clazz, "The clazz is required");

        String schemaString = buildJsonSchema(clazz);
        SchemaDefinition metadata = getDefinition(clazz);

        return removeIgnoreFields(
                metadata.ignoreFields(),
                toJsonNode(schemaString));
    }

    private static JsonNode toJsonNode(String json) {
        try {
            return new ObjectMapper().readValue(json, JsonNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonNode removeIgnoreFields(String[] fieldNames, JsonNode jsonNode) {
        Assert.notNull(jsonNode, "The jsonNode is required");
        Assert.notNull(fieldNames, "The fieldNames is required");

        ObjectNode properties = (ObjectNode) jsonNode.get("properties");
        for (String field : fieldNames) properties.remove(field);
        return jsonNode;
    }

    private static String buildJsonSchema(Class<?> clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            WithoutIdSchemaFactoryWrapper visitor = new WithoutIdSchemaFactoryWrapper();
            mapper.acceptJsonFormatVisitor(clazz, visitor);
            String jsonSchemaString = mapper.writeValueAsString(visitor.finalSchema());

            return removeRecursively(jsonSchemaString,
                    Arrays.asList("additionalInputs", "additionalProperties"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SchemaDefinition getDefinition(Class<?> clazz) {
        try {
            return clazz.getAnnotation(SchemaDefinition.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    private static class WithoutIdSchemaFactoryWrapper extends SchemaFactoryWrapper {

        public WithoutIdSchemaFactoryWrapper() {
            this(null, new WrapperFactory());
        }

        public WithoutIdSchemaFactoryWrapper(SerializerProvider p, WrapperFactory wrapperFactory) {
            super(p, wrapperFactory);
            visitorContext = new VisitorContext() {
                public String javaTypeToUrn(JavaType jt) {
                    return null;
                }
            };
        }
    }
}
