package com.plarium.south.superflow.core.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

public class AvroUtilsInternal {
    public  static final String AVRO_SCHEMA_ENUM_CONVERSION_STRATEGY = "avro.schema.enum.conversion.strategy";

    private AvroUtilsInternal() {}

    public enum EnumConversionStrategy {

        NO_CONVERSION("no_conversion"), ENUM_TO_INT("to_int"), ENUM_TO_STRING("to_string");

        private static Map<String, EnumConversionStrategy> FORMATS = Stream.of(EnumConversionStrategy.values())
                .collect(Collectors.toMap(s -> s.type, Function.identity()));

        @Getter
        private final String type;

        EnumConversionStrategy(String type) {
            this.type = type;
        }

        @JsonCreator
        public static EnumConversionStrategy forVal(String val) {
            return FORMATS.get(val);
        }
    }

    // Wrapper for the Map<String, EnumConversionStrategy> with state and useful methods
    public static class FieldConversion implements Serializable {
        private final Stack<String> fieldPath = new Stack<>();
        private final Map<String, EnumConversionStrategy> perFieldConversion;

        public FieldConversion(Map<String, EnumConversionStrategy> perFieldConversion){
            this.perFieldConversion = perFieldConversion;
        }

        public void pushFieldName(String fieldName){
            fieldPath.push(fieldName);
        }

        public void pop(){
            if (fieldPath.isEmpty()){
                return;
            }

            fieldPath.pop();
        }

        public String peek() { return fieldPath.peek(); }

        public EnumConversionStrategy getFieldStrategy(){
            String currentPath = "";
            for (String field : fieldPath){
                currentPath = currentPath.concat(field);
                if (!perFieldConversion.containsKey(currentPath)){
                    currentPath = currentPath.concat(".");
                    continue;
                }

                return perFieldConversion.get(currentPath);
            }

            return EnumConversionStrategy.NO_CONVERSION;
        }
    }

    /**
     * This method is a copy/paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     * The method allows to convert avro schema to the beam {@link org.apache.beam.sdk.schemas.Schema}
     * @param schema the avro schema to convert
     * @return beam schema
     */
    public static Schema toBeamSchema(@NotNull org.apache.avro.Schema schema, EnumConversionStrategy conversionStrategy){
        Schema.Builder builder = Schema.builder();

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            Schema.Field beamField = toBeamFieldInternal(field, conversionStrategy);
            if (field.doc() != null) {
                beamField = beamField.withDescription(field.doc());
            }
            builder.addField(beamField);
        }

        return builder.build();
    }

    /**
     * The method allows to apply perFieldconversions to the beam schema returning new copy of it the beam
     * {@link org.apache.beam.sdk.schemas.Schema}
     * @param schema the beam schema to convert
     * @param conversion rules
     * @return beam schema
     */
    public static Schema convertBeamSchema(@NotNull Schema schema, FieldConversion conversion){
        return applySchemaConversion(schema, conversion);
    }

    /**
     * This method is a copy/paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
     * conversion. If Schema is not provided, one is inferred from the AVRO schema.
     */
    public static Row toBeamRowStrict(
            GenericRecord record,
            @Nullable Schema schema,
            @Nullable EnumConversionStrategy enumConversionStrategy) {
        if (enumConversionStrategy == null){
            enumConversionStrategy = EnumConversionStrategy.ENUM_TO_INT;
        }

        if (schema == null) {
            schema = toBeamSchema(record.getSchema(), enumConversionStrategy);
        }

        Row.Builder builder = Row.withSchema(schema);
        org.apache.avro.Schema avroSchema = record.getSchema();

        for (Schema.Field field : schema.getFields()) {
            Object value = record.get(field.getName());
            org.apache.avro.Schema fieldAvroSchema = avroSchema.getField(field.getName()).schema();
            builder.addValue(convertAvroFieldStrict(value, fieldAvroSchema, field.getType(), enumConversionStrategy));
        }

        return builder.build();
    }

    /**
     * This method is a copy/paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
     * conversion.
     *
     * @param value {@link GenericRecord} or any nested value
     * @param avroSchema schema for value
     * @param fieldType target beam field type
     * @return value converted for {@link Row}
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static Object convertAvroFieldStrict(
            @Nullable Object value,
            @Nonnull org.apache.avro.Schema avroSchema,
            @Nonnull Schema.FieldType fieldType,
            EnumConversionStrategy enumConversionStrategy) {
        if (value == null) {
            return null;
        }

        TypeWithNullability type = new TypeWithNullability(avroSchema);
        org.apache.avro.Schema.Type avroFieldType = type.type.getType();
        switch (avroFieldType) {
            case ENUM:
                String enumValue = value.toString();
                switch (enumConversionStrategy){
                    case ENUM_TO_INT:
                        return type.type.getEnumOrdinal(enumValue);
                    case ENUM_TO_STRING:
                        return enumValue;
                }
            case RECORD:
                return convertRecordStrict((GenericRecord) value, fieldType, enumConversionStrategy);
            case MAP:
                return convertMapStrict(
                        (Map<CharSequence, Object>) value, type.type.getValueType(), fieldType, enumConversionStrategy);
            case ARRAY:
                return convertArrayStrict((List<Object>) value, type.type.getElementType(), fieldType,
                        enumConversionStrategy);
            default:
                return AvroUtils.convertAvroFieldStrict(value, avroSchema, fieldType);
        }
    }

    private static Schema applySchemaConversion(Schema schema, FieldConversion conversion){
        Schema.Builder builder = Schema.builder();

        for (Schema.Field field : schema.getFields()) {
            Schema.Field beamField = convertField(field, conversion);
            String description = field.getDescription();
            if (description != null) {
                beamField = beamField.withDescription(description);
            }
            builder.addField(beamField);
        }

        return builder.build();
    }

    /**
     * Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     *
     * Converts value to the correct beam array
     * @param values the array values to convert
     * @param elemAvroSchema elements schema
     * @param fieldType beam field type
     * @return converted beam value
     */
    private static Object convertArrayStrict(
            List<Object> values, org.apache.avro.Schema elemAvroSchema,
            Schema.FieldType fieldType,
            EnumConversionStrategy enumConversionStrategy) {
        checkTypeName(fieldType.getTypeName(), Schema.TypeName.ARRAY, "array");

        List<Object> ret = new ArrayList<>(values.size());
        Schema.FieldType elemFieldType = fieldType.getCollectionElementType();

        for (Object value : values) {
            ret.add(convertAvroFieldStrict(value, elemAvroSchema, elemFieldType, enumConversionStrategy));
        }

        return ret;
    }


    /**
     * Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     */
    private static Object convertRecordStrict(GenericRecord record, Schema.FieldType fieldType, EnumConversionStrategy enumConversionStrategy) {
        checkTypeName(fieldType.getTypeName(), Schema.TypeName.ROW, "record");
        return toBeamRowStrict(record, fieldType.getRowSchema(), enumConversionStrategy);
    }

    /**
     * Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     */
    private static Object convertStringStrict(CharSequence value, Schema.FieldType fieldType) {
        checkTypeName(fieldType.getTypeName(), Schema.TypeName.STRING, "string");
        return value.toString();
    }

    /**
     * Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     */
    private static Object convertMapStrict(
            Map<CharSequence, Object> values,
            org.apache.avro.Schema valueAvroSchema,
            Schema.FieldType fieldType,
            EnumConversionStrategy enumConversionStrategy) {
        checkTypeName(fieldType.getTypeName(), Schema.TypeName.MAP, "map");
        checkNotNull(fieldType.getMapKeyType());
        checkNotNull(fieldType.getMapValueType());

        if (!fieldType.getMapKeyType().equals(Schema.FieldType.STRING)) {
            throw new IllegalArgumentException(
                    "Can't convert 'string' map keys to " + fieldType.getMapKeyType());
        }

        Map<Object, Object> ret = new HashMap<>();

        for (Map.Entry<CharSequence, Object> value : values.entrySet()) {
            ret.put(
                    convertStringStrict(value.getKey(), fieldType.getMapKeyType()),
                    convertAvroFieldStrict(value.getValue(), valueAvroSchema, fieldType.getMapValueType(), enumConversionStrategy));
        }

        return ret;
    }

    /**
     * Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     */
    private static void checkTypeName(Schema.TypeName got, Schema.TypeName expected, String label) {
        checkArgument(
                got.equals(expected),
                "Can't convert '" + label + "' to " + got + ", expected: " + expected);
    }

    /**
     * Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     * @param field the avro field to convert
     * @return corresponding beam field
     */
    private static Schema.Field toBeamFieldInternal(org.apache.avro.Schema.Field field, EnumConversionStrategy conversionStrategy) {
        TypeWithNullability nullableType = new TypeWithNullability(field.schema());
        Schema.FieldType beamFieldType = toFieldType(nullableType, conversionStrategy);
        return Schema.Field.of(field.name(), beamFieldType);
    }

    /**
     * Returns copy of the field and applies necessary conversions based on the perFieldConversion
     * @param field the beam field to convert
     * @param conversion the map that contains fields which should be converted
     * @return corresponding beam field
     */
    private static Schema.Field convertField(Schema.Field field,
                                             FieldConversion conversion) {
        String fieldName = field.getName();
        conversion.pushFieldName(fieldName);

        Schema.FieldType fieldType = convertFieldTypeIfNeeded(field.getType(), conversion);
        conversion.pop();
        return Schema.Field.of(fieldName, fieldType);
    }

    private static Schema.FieldType convertFieldTypeIfNeeded(Schema.FieldType fieldType, FieldConversion conversion){
        Schema.TypeName fieldTypeName = fieldType.getTypeName();

        if (fieldTypeName.isCompositeType()){
            return cloneRowFieldAndApplyConversion(fieldType, conversion);
        }

        if (fieldTypeName.isCollectionType()){
            Schema.FieldType elementType = convertFieldTypeIfNeeded(fieldType.getCollectionElementType(), conversion);
            return Schema.FieldType.array(elementType);
        }

        if (fieldTypeName.isMapType()){
            Schema.FieldType valueType = convertFieldTypeIfNeeded(fieldType.getMapValueType(), conversion);
            return Schema.FieldType.map(fieldType.getMapKeyType(), valueType);
        }

        EnumConversionStrategy strategy = conversion.getFieldStrategy();
        if (fieldTypeName.isLogicalType() && strategy != EnumConversionStrategy.NO_CONVERSION){
            Schema.FieldType enumFieldType = null;

            switch (strategy){
                case ENUM_TO_INT:
                    enumFieldType = Schema.FieldType.INT32;
                    break;
                case ENUM_TO_STRING:
                    enumFieldType = Schema.FieldType.STRING;
                    break;
            }

            return enumFieldType;
        }

        return fieldType;
    }

    private static Schema.FieldType cloneRowFieldAndApplyConversion(Schema.FieldType rowType, FieldConversion conversion){
        Schema rowSchema = applySchemaConversion(rowType.getRowSchema(), conversion);
        return Schema.FieldType.row(rowSchema);
    }

    /** Copy paste from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     * Converts AVRO schema to Beam field. */
    private static Schema.FieldType toFieldType(TypeWithNullability type, EnumConversionStrategy conversionStrategy) {
        Schema.FieldType fieldType = null;
        org.apache.avro.Schema avroSchema = type.type;

        LogicalType logicalType = LogicalTypes.fromSchema(avroSchema);
        if (logicalType != null) {
            if (logicalType instanceof LogicalTypes.Decimal) {
                fieldType = Schema.FieldType.DECIMAL;
            } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                // TODO: There is a desire to move Beam schema DATETIME to a micros representation. When
                // this is done, this logical type needs to be changed.
                fieldType = Schema.FieldType.DATETIME;
            } else if (logicalType instanceof LogicalTypes.Date) {
                fieldType = Schema.FieldType.DATETIME;
            }
        }

        if (fieldType == null) {
            switch (type.type.getType()) {
                case RECORD:
                    fieldType = Schema.FieldType.row(toBeamSchema(avroSchema, conversionStrategy));
                    break;

                case ENUM:
                    switch (conversionStrategy){
                        case ENUM_TO_INT:
                            fieldType = Schema.FieldType.INT32;
                            break;
                        case ENUM_TO_STRING:
                            fieldType = Schema.FieldType.STRING;
                            break;
                    }
                    break;

                case ARRAY:
                    Schema.FieldType elementType =
                            toFieldType(new TypeWithNullability(avroSchema.getElementType()), conversionStrategy);
                    fieldType = Schema.FieldType.array(elementType);
                    break;

                case MAP:
                    fieldType =
                            Schema.FieldType.map(
                                    Schema.FieldType.STRING,
                                    toFieldType(new TypeWithNullability(avroSchema.getValueType()), conversionStrategy));
                    break;

                case FIXED:
                    fieldType = AvroUtils.FixedBytesField.fromAvroType(type.type).toBeamType();
                    break;

                case STRING:
                    fieldType = Schema.FieldType.STRING;
                    break;

                case BYTES:
                    fieldType = Schema.FieldType.BYTES;
                    break;

                case INT:
                    fieldType = Schema.FieldType.INT32;
                    break;

                case LONG:
                    fieldType = Schema.FieldType.INT64;
                    break;

                case FLOAT:
                    fieldType = Schema.FieldType.FLOAT;
                    break;

                case DOUBLE:
                    fieldType = Schema.FieldType.DOUBLE;
                    break;

                case BOOLEAN:
                    fieldType = Schema.FieldType.BOOLEAN;
                    break;

                case UNION:
                    throw new IllegalArgumentException("Union types not yet supported");

                case NULL:
                    throw new IllegalArgumentException("Can't convert 'null' to FieldType");

                default:
                    throw new AssertionError("Unexpected AVRO Schema.Type: " + avroSchema.getType());
            }
        }
        fieldType = fieldType.withNullable(type.nullable);
        return fieldType;
    }

    /**
     * This is a copy paste class from {@link org.apache.beam.sdk.schemas.utils.AvroUtils}
     */
    // Unwrap an AVRO schema into the base type an whether it is nullable.
    static class TypeWithNullability {
        public final org.apache.avro.Schema type;
        public final boolean nullable;

        TypeWithNullability(org.apache.avro.Schema avroSchema) {
            if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                List<org.apache.avro.Schema> types = avroSchema.getTypes();

                // optional fields in AVRO have form of:
                // {"name": "foo", "type": ["null", "something"]}

                // don't need recursion because nested unions aren't supported in AVRO
                List<org.apache.avro.Schema> nonNullTypes =
                        types.stream()
                                .filter(x -> x.getType() != org.apache.avro.Schema.Type.NULL)
                                .collect(Collectors.toList());

                if (nonNullTypes.size() == types.size() || nonNullTypes.isEmpty()) {
                    // union without `null` or all 'null' union, keep as is.
                    type = avroSchema;
                    nullable = false;
                } else if (nonNullTypes.size() > 1) {
                    type = org.apache.avro.Schema.createUnion(nonNullTypes);
                    nullable = true;
                } else {
                    // One non-null type.
                    type = nonNullTypes.get(0);
                    nullable = true;
                }
            } else {
                type = avroSchema;
                nullable = false;
            }
        }
    }
}
