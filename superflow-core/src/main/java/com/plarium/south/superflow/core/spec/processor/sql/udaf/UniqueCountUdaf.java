package com.plarium.south.superflow.core.spec.processor.sql.udaf;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.Combine;

import java.util.HashSet;
import java.util.Set;

public class UniqueCountUdaf extends Combine.CombineFn<Object, Set<Integer>, Integer> {
    public static final String NAME = "UCOUNT";

    @Override
    public Set<Integer> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<Integer> addInput(Set<Integer> uniques, Object input) {
        uniques.add(input.hashCode());
        return uniques;
    }

    @Override
    public Set<Integer> mergeAccumulators(Iterable<Set<Integer>> accumulators) {
        Set<Integer> allUniques = new HashSet<>();

        for (Set<Integer> accumulator : accumulators) {
            allUniques.addAll(accumulator);
        }

        return allUniques;
    }

    @Override
    public Integer extractOutput(Set<Integer> accumulator) {
        return accumulator.size();
    }

    @Override
    public Coder<Set<Integer>> getAccumulatorCoder(CoderRegistry registry, Coder<Object> inputCoder) {
        return SetCoder.of(VarIntCoder.of());
    }
}
