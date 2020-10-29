package com.plarium.south.superflow.core.spec.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import static java.lang.String.format;

public class EventTimeBeamRowFn extends DoFn<Row, Row> {

    private final String timeField;

    public EventTimeBeamRowFn(String timeField) {
        this.timeField = timeField;
    }

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<Row> out) {
        final Long mills = fetchTimestamp(row);
        out.outputWithTimestamp(row, Instant.ofEpochMilli(mills));
    }

    private Long fetchTimestamp(Row row) {
        final Long ts;

        try {
            ts = row.getInt64(timeField);
        } catch (IllegalStateException e) {
            String msg = format("Schema not contains timestamp field '%s' in row: %s", timeField, row);
            throw new SetRowEventTimeException(msg);
        }

        if(ts == null) {
            String msg = format("Timestamp value is null for field '%s' in row: %s", timeField, row);
            throw new SetRowEventTimeException(msg);
        }

        return ts;
    }

    private static class SetRowEventTimeException extends RuntimeException {
        SetRowEventTimeException(String message) {
            super(message);
        }
    }
}
