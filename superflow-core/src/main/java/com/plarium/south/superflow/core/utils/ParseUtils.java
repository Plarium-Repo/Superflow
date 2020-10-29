package com.plarium.south.superflow.core.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

public class ParseUtils {

    private ParseUtils() {}

    public static <T> T parseYaml(Path path, Class<T> clazz) throws IOException {
        return parseYaml(PathUtils.toBytes(path), clazz);
    }
    public static <T> T parseYaml(InputStream input, Class<T> clazz) throws IOException {
        return parseYaml(IOUtils.toByteArray(input), clazz);
    }
    public static <T> T parseYaml(byte[] data, Class<T> clazz) throws IOException {
        YAMLFactory yaml = new YAMLFactory();
        ObjectMapper mapper = new ObjectMapper(yaml);
        YAMLParser parser = yaml.createParser(data);
        return mapper.readValue(parser, clazz);
    }


    public static <T> List<T> parseYaml(Path path, TypeReference<T> clazz) throws IOException {
        return parseYaml(PathUtils.toBytes(path), clazz);
    }
    public static <T> List<T> parseYaml(InputStream input, TypeReference<T> clazz) throws IOException {
        return parseYaml(IOUtils.toByteArray(input), clazz);
    }
    public static <T> List<T> parseYaml(byte[] data, TypeReference<T> clazz) throws IOException {
        YAMLFactory yaml = new YAMLFactory();
        ObjectMapper mapper = new ObjectMapper(yaml);
        YAMLParser parser = yaml.createParser(data);
        return Lists.newArrayList(mapper.readValues(parser, clazz));
    }


    public static <T> T parseJson(Path path, Class<T> clazz) throws IOException {
        return parseJson(PathUtils.toBytes(path), clazz);
    }
    public static <T> T parseJson(InputStream input, Class<T> clazz) throws IOException {
        return parseJson(IOUtils.toByteArray(input), clazz);
    }
    public static <T> T parseJson(byte[] data, Class<T> clazz) throws IOException {
        JsonFactory json = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(json);
        JsonParser parser = json.createParser(data);
        return mapper.readValue(parser, clazz);
    }


    public static <T> List<T> parseJson(Path path, TypeReference<T> clazz) throws IOException {
        return parseJson(PathUtils.toBytes(path), clazz);
    }
    public static <T> List<T> parseJson(InputStream input, TypeReference<T> clazz) throws IOException {
        return parseJson(IOUtils.toByteArray(input), clazz);
    }
    public static <T> List<T> parseJson(byte[] data, TypeReference<T> clazz) throws IOException {
        JsonFactory json = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(json);
        JsonParser parser = json.createParser(data);
        return Lists.newArrayList(mapper.readValues(parser, clazz));
    }

    public static String jsonToYaml(String json) {
        try {
            JsonNode jsonNode = new ObjectMapper().readValue(json, JsonNode.class);
            return new ObjectMapper(new YAMLFactory()).writeValueAsString(jsonNode);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String yamlToJson(String yaml) {
        try {
            JsonNode jsonNode = new ObjectMapper(new YAMLFactory()).readValue(yaml, JsonNode.class);
            return new ObjectMapper().writeValueAsString(jsonNode);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonNode jsonToNode(String json) {
        return jsonToNode(json.getBytes());
    }

    public static JsonNode jsonToNode(byte[] json) {
        try {
            return new ObjectMapper().readValue(json, JsonNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
