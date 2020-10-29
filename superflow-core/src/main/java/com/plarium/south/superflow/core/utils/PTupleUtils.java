package com.plarium.south.superflow.core.utils;

import org.apache.beam.sdk.values.TupleTag;

import java.util.Set;
import java.util.stream.Collectors;

public class PTupleUtils {

    private PTupleUtils() {}

    public static  <T> Set<TupleTag<T>> materializeTags(Set<TupleTag<?>> tupleTags) {
        return tupleTags.stream()
                .map(t -> new TupleTag<T>(t.getId()))
                .collect(Collectors.toSet());
    }
}
