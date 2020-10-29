package com.plarium.south.superflow.core.utils;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.List;
import java.util.stream.Collectors;

public class JsonPathUtils {

    private JsonPathUtils() {}

    public static String removeRecursively(String json, List<String> fields) {
        DocumentContext context = JsonPath.parse(json);
        for (String field : fields) {
            context = context.delete("$..*." + field);
        }

        return context.jsonString();
    }

    public static List<JsonPath> compilePaths(List<String> stringPaths) {
        return stringPaths.stream()
                .map(p -> JsonPath.compile(p))
                .collect(Collectors.toList());
    }
}
