package com.plarium.south.superflow.core.spec.sink.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Getter;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.firstNonNull;

public class HttpConfig implements Serializable {

    private static final String DEFAULT_TIMEOUT = "300 ms";
    private static final Integer DEFAULT_NUMBER_OF_RETRIES = 10;
    private static final Integer DEFAULT_CONTENT_LIMIT = 0x4000;


    @Getter
    @JsonPropertyDescription("The read timeout in format <number <ms|sec|min|hour|day>>, default: 300 ms")
    private final String readTimeout;

    @Getter
    @JsonPropertyDescription("The write timeout in format <number <ms|sec|min|hour|day>>, default: 300 ms")
    private final String writeTimeout;

    @Getter
    @JsonPropertyDescription("The connection timeout in format <number <ms|sec|min|hour|day>>, default: 300 ms")
    private final String connectTimeout;

    @Getter
    @JsonPropertyDescription("The maximum number of retries for http execution, default: 10")
    private final Integer numberOfRetries;

    @Getter
    @JsonPropertyDescription("The time interval between try failed http requests, default: 300 ms")
    private final String failedRetryTimeout;

    @Getter
    @JsonPropertyDescription("Enable http requests logging, default: true")
    private final Boolean enableLogging;

    @Getter
    @JsonPropertyDescription("Enable follow redirects, default: false")
    private final Boolean followRedirects;

    @Getter
    @JsonPropertyDescription("Set the limit to the content size that will be logged, default: 16384 (16 kb) ")
    private final Integer contentLoggingLimit;


    @JsonCreator
    public HttpConfig(
            @JsonProperty(value = "readTimeout") String readTimeout,
            @JsonProperty(value = "writeTimeout") String writeTimeout,
            @JsonProperty(value = "enableLogging") Boolean enableLogging,
            @JsonProperty(value = "connectTimeout") String connectTimeout,
            @JsonProperty(value = "followRedirects") Boolean followRedirects,
            @JsonProperty(value = "numberOfRetries") Integer numberOfRetries,
            @JsonProperty(value = "failedRetryTimeout") String failedRetryTimeout,
            @JsonProperty(value = "contentLoggingLimit") Integer contentLoggingLimit)
    {
        this.enableLogging = firstNonNull(enableLogging, true);
        this.followRedirects = firstNonNull(followRedirects, false);
        this.readTimeout = firstNonNull(readTimeout, DEFAULT_TIMEOUT);
        this.writeTimeout = firstNonNull(writeTimeout, DEFAULT_TIMEOUT);
        this.connectTimeout = firstNonNull(connectTimeout, DEFAULT_TIMEOUT);
        this.failedRetryTimeout = firstNonNull(failedRetryTimeout, DEFAULT_TIMEOUT);
        this.numberOfRetries = firstNonNull(numberOfRetries, DEFAULT_NUMBER_OF_RETRIES);
        this.contentLoggingLimit = firstNonNull(contentLoggingLimit, DEFAULT_CONTENT_LIMIT);
    }

    public static HttpConfig defaultHttpConfig() {
        return new HttpConfig(
                DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT,
                true,
                DEFAULT_TIMEOUT,
                false,
                DEFAULT_NUMBER_OF_RETRIES,
                DEFAULT_TIMEOUT,
                DEFAULT_CONTENT_LIMIT);
    }
}
