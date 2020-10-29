package com.plarium.south.superflow.core.spec.sink.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.mapping.BaseMapping;
import com.plarium.south.superflow.core.spec.sink.BasePTupleRowSink;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SINK;
import static com.plarium.south.superflow.core.spec.sink.http.HttpConfig.defaultHttpConfig;

@SchemaDefinition(
        type = SINK,
        ignoreFields = {"window"},
        required = {"method", "urlTemplate", "contentType", "mapping"})

@JsonPropertyOrder({"type", "name", "window", "method", "contentType", "mapping", "urlTemplate", "httpConfig", "bodyFilter", "headers", "bodyTemplate"})
public class HttpSink  extends BasePTupleRowSink {
    public static final String TYPE = "sink/http";

    @Getter @NotNull
    @JsonPropertyDescription("The http method type: [post, put, patch]")
    private final RequestMethod method;

    @Getter @NotNull
    @JsonPropertyDescription("The http request Content-Type")
    private final String contentType;

    @Getter @NotNull
    @JsonPropertyDescription("The input data mapping")
    private final BaseMapping mapping;

    @Getter @NotNull
    @JsonPropertyDescription("The data driven template for http request, example: http://endpoint/books/%{bookId} where bookId field from payload")
    private final String urlTemplate;

    @Getter @NotNull
    @JsonPropertyDescription("The http low level configuration, timeouts, retries, error tolerance, etc..")
    private final HttpConfig httpConfig;

    @Getter
    @JsonPropertyDescription("The set strings of jsonpaths for exclude from payload, will be ignored if using bodyTemplate")
    private final Set<String> bodyFilter;

    @Getter
    @JsonPropertyDescription("The http headers data driven template (like as urlTemplate) in map<string, string> format, example: 'key': '%{val}'")
    private final Map<String, String> headers;

    @Getter
    @JsonPropertyDescription("The http body data driven template like as urlTemplate, any string format")
    private final String bodyTemplate;


    @JsonCreator
    protected HttpSink(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "httpConfig") HttpConfig httpConfig,
            @JsonProperty(value = "bodyTemplate") String bodyTemplate,
            @JsonProperty(value = "bodyFilter") Set<String> bodyFilter,
            @JsonProperty(value = "headers") Map<String, String> headers,
            @JsonProperty(value = "method", required = true) RequestMethod method,
            @JsonProperty(value = "mapping", required = true) BaseMapping mapping,
            @JsonProperty(value = "contentType", required = true) String contentType,
            @JsonProperty(value = "urlTemplate", required = true) String urlTemplate)
    {
        super(name, null, false, window);
        this.method = method;
        this.mapping = mapping;
        this.bodyFilter = bodyFilter;
        this.urlTemplate = urlTemplate;
        this.contentType = contentType;
        this.headers = firstNonNull(headers, Maps.newHashMap());
        this.httpConfig = firstNonNull(httpConfig, defaultHttpConfig());
        this.bodyTemplate = bodyTemplate;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean windowIsNeed() {
        return false;
    }

    @Override
    public boolean registryIsNeed() {
        return false;
    }

    @Override
    public PDone expand(PCollectionTuple input) {
        String tag = mapping.getTag();
        PCollection<Row> dataset = getByTag(input, tag);

        dataset = applyWindow(dataset);
        dataset = applyTimePolicy(dataset, mapping.getEventTime());

        Schema schema = dataset.getSchema();
        dataset.apply(buildWriter(schema));

        return PDone.in(input.getPipeline());
    }

    private PTransform<PCollection<? extends Row>, PCollection<Void>> buildWriter(Schema schema) {
        return ParDo.of(new WriteHttpFn(schema, urlTemplate, contentType, method.name(), httpConfig, bodyTemplate, bodyFilter, headers));
    }
}