package com.plarium.south.superflow.core.spec.processor.jsonp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.Option;
import com.plarium.south.superflow.core.spec.commons.jsonpath.JsonPathPipeline;
import com.plarium.south.superflow.core.spec.commons.jsonpath.operator.JsonPathOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.RowJsonSerializer;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

@Slf4j
public class JsonPathStringDoFn extends DoFn<Row, Row> {

    private final String select;

    private final Schema inputSchema;

    private final Schema outputSchema;

    private final Set<Option> options;

    private final List<JsonPathOperator> ops;

    private final boolean ignoreEmpty;

    private transient ObjectMapper inputMapper;

    private transient ObjectMapper outputMapper;

    private transient JsonPathPipeline jsonpPipeline;


    public JsonPathStringDoFn(
            String select,
            Schema inputSchema,
            Schema outputSchema,
            Set<Option> options,
            List<JsonPathOperator> ops,
            boolean ignoreEmpty)
    {
        this.ops = ops;
        this.select = select;
        this.options = options;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.ignoreEmpty = ignoreEmpty;
    }

    @Setup
    public void setup() {
        inputMapper = newObjectMapperWith(RowJsonSerializer.forSchema(inputSchema));
        outputMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(outputSchema));

        jsonpPipeline = new JsonPathPipeline.Builder()
                .select(select)
                .operators(ops)
                .options(options)
                .build();
    }

    @ProcessElement
    public void process(@Element Row row, ProcessContext context) throws IOException {
        String json = inputMapper.writeValueAsString(row);
        DocumentContext doc = jsonpPipeline.process(json);
        String resultJson = doc.jsonString();

        if (!ignoreEmpty && emptyJson(resultJson)) {
            throw new IllegalStateException("jsonpath pipeline return empty object");
        }

        if (emptyJson(resultJson)) {
            return;
        }

        if (resultJson.startsWith("[")) {
            List<Object> array = doc.read("$");

            for (Object obj : array) {
                String jsonObj = outputMapper.writeValueAsString(obj);
                context.output(outputMapper.readValue(jsonObj, Row.class));
            }
        }
        else {
            Row outputRow = outputMapper.readValue(resultJson, Row.class);
            context.output(outputRow);
        }
    }

    private boolean emptyJson(String json) {
        return "{}".equals(json) || "[]".equals(json);
    }
}
