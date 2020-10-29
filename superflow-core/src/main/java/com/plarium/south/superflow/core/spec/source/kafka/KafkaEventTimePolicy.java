package com.plarium.south.superflow.core.spec.source.kafka;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.util.Optional;

public class KafkaEventTimePolicy extends TimestampPolicy<byte[], Row> {

    private String timeField;

    private Instant watermark;

    public KafkaEventTimePolicy(String timeField, Optional<Instant> previousWatermark) {
        this.timeField = timeField;
        this.watermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<byte[], Row> record) {
        Long ts = record.getKV().getValue().getInt64(timeField);
        watermark = Instant.ofEpochMilli(ts);
        return watermark;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return watermark;
    }
}
