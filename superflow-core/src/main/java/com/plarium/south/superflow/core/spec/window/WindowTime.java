package com.plarium.south.superflow.core.spec.window;

import org.joda.time.Duration;

import java.util.Arrays;

import static org.joda.time.Duration.*;

public enum WindowTime {
    ms, sec, min, hour, day;

    public Duration getDuration(long duration) {
        switch (this) {
            case ms: return millis(duration);

            case sec: return standardSeconds(duration);

            case min: return standardMinutes(duration);

            case hour: return standardHours(duration);

            case day: return standardDays(duration);

            default: {
                String msg = "Invalid window time unit value, possible values: %s";
                throw new IllegalArgumentException(String.format(msg, Arrays.toString(WindowTime.values())));
            }
        }
    }
}
