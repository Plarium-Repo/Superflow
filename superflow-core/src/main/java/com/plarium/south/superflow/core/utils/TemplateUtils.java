package com.plarium.south.superflow.core.utils;

import org.apache.commons.lang.text.StrSubstitutor;
import org.springframework.util.Assert;
import java.util.regex.*;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

public class TemplateUtils {
    private static final String INCORRECT_FORMAT_MESSAGE = "Cannot parse value '%s'. Value is in incorrect format, please " +
            "use following format: ${OS_VAR_NAME[:<default value>]}";

    private static final Pattern ValuePattern = Pattern.compile("^\\$\\{(?<osEnvKey>[\\w]+):?(?<defaultVal>.*)?}$",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);


    private TemplateUtils() {}

    public static String renderYamlSpec(String text, Map<String, String> variables) {
        return StrSubstitutor.replace(text, variables, "${", "}");
    }

    public static String renderTextInRuntime(String text, Map<String, String> variables) {
        return StrSubstitutor.replace(text, variables, "%{", "}");
    }

    public static String getOrDefaultForOsVariable(String rawKey) {
        Matcher matcher = ValuePattern.matcher(rawKey);
        Assert.isTrue(matcher.matches(), String.format(INCORRECT_FORMAT_MESSAGE, rawKey));

        String osEnvKey = matcher.group("osEnvKey");
        String defaultVal = matcher.group("defaultVal");

        String osEnvVal = System.getenv(osEnvKey);
        if (osEnvVal == null && defaultVal == null) {
            throw new OSEnvNotFoundException(rawKey);
        }

        return firstNonNull(osEnvVal, defaultVal);
    }

    private static class OSEnvNotFoundException extends RuntimeException {
        private static final String ERROR_TEMPLATE = "Not found OS env variable and not set default for key: '%s'";

        public OSEnvNotFoundException(String rawValue) {
            super(String.format(ERROR_TEMPLATE, rawValue));
        }
    }
}
