package com.plarium.south.superflow.core.utils;

import com.plarium.south.superflow.core.spec.dofn.EventTimeBeamRowFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class WindowUtils {

    public static PCollection<Row> applyTimePolicy(PCollection<Row> dataset, String fieldName) {
        return dataset.apply(ParDo.of(new EventTimeBeamRowFn(fieldName)));
    }
}
