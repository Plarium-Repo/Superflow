package com.plarium.south.superflow.core.spec.sink.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.BackOff;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.plarium.south.superflow.core.utils.DurationUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static com.plarium.south.superflow.core.utils.TemplateUtils.renderTextInRuntime;
import static java.net.URLEncoder.encode;
import static org.apache.beam.sdk.util.RowJson.RowJsonSerializer.forSchema;
import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

@Slf4j
public class WriteHttpFn extends DoFn<Row, Void> {
    private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.toString();


    private final Schema schema;

    private final String urlTemplate;

    private final String contentType;

    private final String requestMethod;

    private final HttpConfig httpConfig;

    private final String bodyTemplate;

    private final Map<String, String> headers;

    private final Set<String> bodyFilter;


    private transient ObjectMapper mapper;

    private transient HttpTransport transport;

    private transient HttpRequestFactory requestFactory;


    public WriteHttpFn(
            Schema schema,
            String urlTemplate,
            String contentType,
            String requestMethod,
            HttpConfig httpConfig,
            @Nullable String bodyTemplate,
            @Nullable Set<String> bodyFilter,
            @Nullable Map<String, String> headers)
    {
        checkNotNull(schema, "The schema is required.");
        checkNotNull(httpConfig, "The httpConfig is required.");
        checkNotNull(contentType, "The contentType is required.");
        checkNotNull(urlTemplate, "The urlTemplate is required.");
        checkNotNull(requestMethod, "The requestMethod is required.");

        this.schema = schema;
        this.headers = headers;
        this.httpConfig = httpConfig;
        this.contentType = contentType;
        this.urlTemplate = urlTemplate;
        this.bodyTemplate = bodyTemplate;
        this.requestMethod = requestMethod;
        this.bodyFilter = firstNonNull(bodyFilter, newHashSet());
    }

    @Setup
    public void setup() {
        transport = new NetHttpTransport(); //TODO: switch transport for poolable apache http client
        mapper = newObjectMapperWith(forSchema(schema));

        requestFactory = transport.createRequestFactory(request -> {
            request.setThrowExceptionOnExecuteError(true);
            request.setLoggingEnabled(httpConfig.getEnableLogging());
            request.setCurlLoggingEnabled(httpConfig.getEnableLogging());
            request.setNumberOfRetries(httpConfig.getNumberOfRetries());
            request.setFollowRedirects(httpConfig.getFollowRedirects());
            request.setReadTimeout(toDuration(httpConfig.getReadTimeout()));
            request.setWriteTimeout(toDuration(httpConfig.getWriteTimeout()));
            request.setContentLoggingLimit(httpConfig.getContentLoggingLimit());
            request.setConnectTimeout(toDuration(httpConfig.getConnectTimeout()));

            HttpErrorRetryInterval errorRetryInterval =
                    new HttpErrorRetryInterval(httpConfig.getFailedRetryTimeout());

            request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(errorRetryInterval));
            request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(errorRetryInterval));
        });
    }

    @ProcessElement
    public void processElement(@Element Row row) throws IOException {
        Map<String, String> templateVars = buildTemplateVars(row);
        HttpRequest request = buildRequest(row, templateVars);
        HttpResponse response = request.execute();
        response.disconnect();
    }

    @Teardown
    public void teardown() throws Exception {
        transport.shutdown();
    }


    private String buildUrl(String url, Map<String, String> templateVars) {
        return renderTextInRuntime(url, templateVars);
    }

    private HttpRequest buildRequest(Row row, Map<String, String> templateVars) throws IOException {
        byte[] jsonBytes = buildRequestBody(row, templateVars);
        GenericUrl url = new GenericUrl(buildUrl(urlTemplate, templateVars));
        ByteArrayContent content = new ByteArrayContent(contentType, jsonBytes);
        HttpRequest request = requestFactory.buildRequest(requestMethod, url, content);
        request.setHeaders(buildHeaders(templateVars));
        return request;
    }

    private byte[] buildRequestBody(Row row, Map<String, String> templateVars) throws JsonProcessingException, UnsupportedEncodingException {
        if (bodyTemplate != null) {
            return toBytes(renderTextInRuntime(bodyTemplate, templateVars));
        }

        String jsonRow = mapper.writeValueAsString(row);

        if (bodyFilter.isEmpty()) {
            return toBytes(jsonRow);
        }

        DocumentContext doc = JsonPath.parse(jsonRow);
        bodyFilter.forEach(p -> doc.delete(p));
        return toBytes(doc.jsonString());
    }

    private HttpHeaders buildHeaders(Map<String, String> templateVars) throws UnsupportedEncodingException {
        if (headers == null || headers.isEmpty()) {
            return new HttpHeaders();
        }

        HttpHeaders httpHeaders = new HttpHeaders();

        for (String key : headers.keySet()) {
            String rawVal = headers.get(key);
            String renderedVal = renderTextInRuntime(rawVal, templateVars);
            httpHeaders.put(key, encodeUtf(renderedVal));
        }

        return httpHeaders;
    }

    private Map<String, String> buildTemplateVars(Row row) throws UnsupportedEncodingException {
        Map<String, String> templateVars = new HashMap<>();
        List<Schema.Field> fields = row.getSchema().getFields();

        for (Schema.Field field : fields) {
            String name = field.getName();
            Object value = row.getValue(name);

            if (value == null) continue;

            templateVars.put(name, encodeUtf(value));
        }

        return templateVars;
    }


    private static Integer toDuration(String val) {
        checkNotNull(val);
        return (int) DurationUtils.parse(val).getMillis();
    }

    private static byte[] toBytes(String json) throws UnsupportedEncodingException {
        checkNotNull(json);
        return json.getBytes(DEFAULT_ENCODING);
    }

    private static String encodeUtf(Object val) throws UnsupportedEncodingException {
        checkNotNull(val);
        return encode(val.toString(), DEFAULT_ENCODING);
    }



    private final static class HttpErrorRetryInterval implements BackOff {

        private final int backOffMillis;


        public HttpErrorRetryInterval(String duration) {
            backOffMillis = toDuration(duration);
        }

        @Override
        public void reset() {}

        @Override
        public long nextBackOffMillis() {
            return backOffMillis;
        }
    }
}
