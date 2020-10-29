package com.plarium.south.superflow.core.utils;

import com.plarium.south.superflow.core.spec.window.WindowTime;
import org.joda.time.Duration;

public class DurationUtils {

    private DurationUtils() {}

    /**
     * @param value in format: 'number <sec|min|hour|day>
     * @return {@link Duration}
     */
    public static Duration parse(String value) {
        String[] splitter = value.split(" ");
        long duration = Long.parseLong(splitter[0]);
        WindowTime unit = WindowTime.valueOf(splitter[1]);
        return unit.getDuration(duration);
    }
}
