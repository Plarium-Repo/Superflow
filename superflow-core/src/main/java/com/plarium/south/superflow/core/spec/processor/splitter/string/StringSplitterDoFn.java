package com.plarium.south.superflow.core.spec.processor.splitter.string;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class StringSplitterDoFn extends DoFn<Row, Row> {
    private static final String EMPTY_STRING = "";

    private final String splitter;

    private final boolean dropInput;

    private final String inputField;

    private final Schema inputSchema;

    private final Schema outputSchema;

    private final CastVariableType castElementType;


    public StringSplitterDoFn(
            String inputField,
            String splitter,
            boolean dropInput,
            Schema inputSchema,
            Schema outputSchema,
            CastVariableType castElementType)
    {
        this.inputField = inputField;
        this.splitter = splitter;
        this.dropInput = dropInput;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.castElementType = castElementType;
    }


    @ProcessElement
    @SuppressWarnings("ConstantConditions")
    public void processElement(@Element Row row, ProcessContext context) {
        List<Object> values = new ArrayList<>();
        List<Schema.Field> fields = inputSchema.getFields();

        for (Schema.Field field : fields) {
            if (needAddValueInRow(field)) {
                values.add(row.getValue(field.getName()));
            }
        }

        String value = row.getString(inputField);
        List<Object> outputValue = buildArrayValue(value);

        Row outputRow = Row.withSchema(outputSchema)
                .addValues(values)
                .addArray(outputValue)
                .build();

        context.output(outputRow);
    }

    private boolean needAddValueInRow(Schema.Field field) {
        return !(Objects.equals(field.getName(), inputField) && dropInput);
    }

    private List<Object> buildArrayValue(String value) {
        String[] splittedArray = value.split(splitter);

        return Arrays.stream(splittedArray)
                .filter(s -> !EMPTY_STRING.equals(s))
                .map(this::castArrayElements)
                .collect(toList());
    }

    private Object castArrayElements(String val) {
        switch (castElementType) {
            case INT: return Integer.valueOf(val);
            case LONG: return Long.valueOf(val);
            case DOUBLE: return Double.valueOf(val);
            case STRING: return val;
            default:  throw new IllegalStateException("Unexpected value: " + castElementType);
        }
    }
}
