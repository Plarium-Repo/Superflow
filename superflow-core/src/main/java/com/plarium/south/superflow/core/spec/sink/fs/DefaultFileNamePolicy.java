package com.plarium.south.superflow.core.spec.sink.fs;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.*;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;

import java.util.UUID;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.joda.time.format.DateTimeFormat.forPattern;


public class DefaultFileNamePolicy extends FilenamePolicy {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    private final String format;

    private final String location;

    private final String extension;

    private transient DateTimeFormatter dateFormatter;



    public DefaultFileNamePolicy(String format, String location, String extension) {
        this.format = firstNonNull(format, DEFAULT_FORMAT);
        this.location = location;
        this.extension = extension;
    }


    @Override
    public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, OutputFileHints outputFileHints) {
        String filename = getWindowedFileName((IntervalWindow) window);
        return FileBasedSink.convertToFileResourceIfPossible(filename);
    }

    @Override
    public ResourceId unwindowedFilename(int shardNumber, int numShards, OutputFileHints outputFileHints) {
        return FileBasedSink.convertToFileResourceIfPossible(getUnwindowedFileName());
    }


    private String getWindowedFileName(IntervalWindow window) {
        return String.format("%s/%s/%s.%s", location, format(window.start()), uuid(), extension);
    }

    private String getUnwindowedFileName() {
        return String.format("%s/%s.%s", location, uuid(), extension);
    }

    private String format(Instant start) {
        if (dateFormatter == null) {
            dateFormatter = forPattern(format);
        }

        return dateFormatter.print(start);
    }

    private String uuid() {
        return UUID.randomUUID().toString();
    }
}
