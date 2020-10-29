package com.plarium.south.superflow.core.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.beam.sdk.schemas.utils.AvroUtils.convertAvroFieldStrict;
import static org.apache.beam.sdk.schemas.utils.AvroUtils.toBeamRowStrict;


public class BeamRowUtils {

    private BeamRowUtils() {}

    public static Schema mergeSchemas(Schema left, Schema right) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.addAll(left.getFields());
        fields.addAll(right.getFields());

        return Schema.builder()
                .addFields(fields)
                .build();
    }

    public static Row mergeRows(Row left, Row right, Schema mergeSchema) {
        List<Object> values = new ArrayList<>();
        values.addAll(left.getValues());
        values.addAll(right.getValues());

        return Row.withSchema(mergeSchema)
                .addValues(values)
                .build();
    }

    public static Row mergeRows(Schema mergeSchema, Row... rows) {
        List<Object> values = new ArrayList<>();

        for (Row row : rows) {
            values.addAll(row.getValues());
        }

        return Row.withSchema(mergeSchema)
                .addValues(values)
                .build();
    }

    public static Row mergeRows(Schema mergeSchema, List<Row> rows) {
        List<Object> values = new ArrayList<>();

        for (Row row : rows) {
            values.addAll(row.getValues());
        }

        return Row.withSchema(mergeSchema)
                .addValues(values)
                .build();
    }

    public static Row toBeamRow(GenericRecord record, Schema schema, boolean allowNoStrict) {
        if (!allowNoStrict) return toBeamRowStrict(record, schema);

        Row.Builder builder = Row.withSchema(schema);
        org.apache.avro.Schema avroSchema = record.getSchema();

        for (Schema.Field field : schema.getFields()) {
            String beamFieldName = field.getName();
            Schema.FieldType fieldType = field.getType();

            Object value = record.get(beamFieldName);
            org.apache.avro.Schema.Field avroField = avroSchema.getField(beamFieldName);

            if (avroField == null) {
                avroField = org.apache.beam.sdk.schemas.utils.AvroUtils.toAvroField(field, avroSchema.getNamespace());
            }

            if (value == null) {
                Boolean nullable = field.getType().getNullable();
                value = nullable ? null : getTypeDefaultValue(fieldType.getTypeName());
            }

            org.apache.avro.Schema fieldAvroSchema = avroField.schema();
            builder.addValue(convertAvroFieldStrict(value, fieldAvroSchema, field.getType()));
        }

        return builder.build();
    }

    public static Row toBeamRow(
            GenericRecord record,
            Schema schema,
            boolean allowNoStrict,
            AvroUtilsInternal.EnumConversionStrategy enumConversionStrategy) {
        if (!allowNoStrict) return AvroUtilsInternal.toBeamRowStrict(record, schema, enumConversionStrategy);

        Row.Builder builder = Row.withSchema(schema);
        org.apache.avro.Schema avroSchema = record.getSchema();

        for (Schema.Field field : schema.getFields()) {
            String beamFieldName = field.getName();
            Schema.FieldType fieldType = field.getType();

            Object value = record.get(beamFieldName);
            org.apache.avro.Schema.Field avroField = avroSchema.getField(beamFieldName);

            if (avroField == null) {
                avroField = org.apache.beam.sdk.schemas.utils.AvroUtils.toAvroField(field, avroSchema.getNamespace());
            }

            if (value == null) {
                Boolean nullable = field.getType().getNullable();
                value = nullable ? null : getTypeDefaultValue(fieldType.getTypeName());
            }

            org.apache.avro.Schema fieldAvroSchema = avroField.schema();
            builder.addValue(AvroUtilsInternal.convertAvroFieldStrict(value, fieldAvroSchema, field.getType(), enumConversionStrategy));
        }

        return builder.build();
    }

    public static Object getTypeDefaultValue(Schema.TypeName typeName) {
        switch (typeName) {
            case BYTE:
                return (byte)0;
            case INT16:
                return (short)0;
            case INT32:
                return 0;
            case INT64:
                return 0L;
            case DOUBLE:
                return 0.0d;
            case FLOAT:
                return 0.0f;
            case BOOLEAN:
                return false;
            case STRING:
                return "UNDEFINED";
            case BYTES:
                return new byte[0];
            case MAP:
                return new HashMap<String, Object>();
            case ARRAY:
                return com.google.common.collect.Lists.newArrayList();
            default:
                throw new IllegalStateException("Unsupported default value for type: " + typeName);
        }
    }
}
