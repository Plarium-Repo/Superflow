package com.plarium.south.superflow.core.spec.processor.sql.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.joda.time.format.DateTimeFormat;

public class StringDateToLongUdf implements BeamSqlUdf {
    public static final String NAME = "FDT";

    public Long eval(String input, String format) {
        return DateTimeFormat
                .forPattern(format)
                .withZoneUTC()
                .parseMillis(input);
    }
}
