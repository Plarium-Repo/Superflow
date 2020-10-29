package com.plarium.south.superflow.core.utils;

import com.google.common.base.Splitter;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.beam.sdk.io.FileSystems.matchNewResource;

@Slf4j
public class ClassLoaderUtils {

    private ClassLoaderUtils() {}


    public static URLClassLoader getNewClassLoader(String paths) {
        final String destRoot = Files.createTempDir().getAbsolutePath();
        List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(paths);

        URL[] urls =
                listOfJarPaths
                        .stream()
                        .map(
                                jarPath -> {
                                    try {
                                        ResourceId sourceResourceId = matchNewResource(jarPath, false);
                                        File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                                        ResourceId destResourceId = matchNewResource(destFile.getAbsolutePath(), false);
                                        copyResourceById(sourceResourceId, destResourceId);
                                        log.info("Localized jar: {} to {}", sourceResourceId, destResourceId);
                                        return destFile.toURI().toURL();
                                    } catch (IOException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                })
                        .toArray(URL[]::new);

        return URLClassLoader.newInstance(urls);
    }

    private static void copyResourceById(ResourceId source, ResourceId dest) throws IOException {
        try (ReadableByteChannel rbc = FileSystems.open(source)) {
            try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
                ByteStreams.copy(rbc, wbc);
            }
        }
    }
}
