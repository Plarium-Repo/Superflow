package com.plarium.south.superflow.core.spec.processor.sql.udaf;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;

public class AnyLongAggUdaf extends Combine.CombineFn<Long, Long, Long> {
    public static final String NAME = "ANYLONG";

    @Override
    public Long createAccumulator() {
        return Long.MIN_VALUE;
    }

    @Override
    public Long addInput(Long accum, Long input) {
        if (accum.equals(Long.MIN_VALUE)) {
            accum = input;
        }

        return accum;
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accumulators) {
        return accumulators.iterator().next();
    }

    @Override
    public Long extractOutput(Long accumulator) {
        return accumulator;
    }

    @Override
    public Coder<Long> getAccumulatorCoder(CoderRegistry registry, Coder<Long> inputCoder) {
        return VarLongCoder.of();
    }
}
