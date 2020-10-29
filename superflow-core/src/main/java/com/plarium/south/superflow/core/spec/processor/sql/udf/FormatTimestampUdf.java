package com.plarium.south.superflow.core.spec.processor.sql.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.joda.time.format.DateTimeFormat;

public class FormatTimestampUdf implements BeamSqlUdf {
    public static final String NAME = "FTS";

    public String eval(Long input, String format) {
        return DateTimeFormat
                .forPattern(format)
                .withZoneUTC()
                .print(input);
    }
}
