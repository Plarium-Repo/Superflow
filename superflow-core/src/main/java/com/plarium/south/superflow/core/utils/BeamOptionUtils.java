package com.plarium.south.superflow.core.utils;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class BeamOptionUtils {

    private BeamOptionUtils() {}

    public static <T extends PipelineOptions> T createOptionsStrict(String[] args, Class<T> clazz) {
        return PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(clazz);
    }

    public static <T extends PipelineOptions> T createOptionsNoStrict(String[] args, Class<T> clazz) {
        return PipelineOptionsFactory
                .fromArgs(args)
                .withoutStrictParsing()
                .as(clazz);
    }
}
