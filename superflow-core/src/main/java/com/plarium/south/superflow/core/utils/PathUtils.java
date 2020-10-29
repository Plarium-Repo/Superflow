package com.plarium.south.superflow.core.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtils {

    private PathUtils() {}

    public static Path getPath(String value) {
        if (value.contains("://")) {
            try {
                return Paths.get(new URI(value));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            return Paths.get(value);
        }
    }

    public static byte[] toBytes(Path path) throws IOException {
        return Files.readAllBytes(path);
    }

    public static String getContentAsString(Path path) throws IOException {
        return String.join("\n", Files.readAllLines(path));
    }
}
