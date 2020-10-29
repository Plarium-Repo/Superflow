package com.plarium.south.superflow.core.spec.processor.transform.avro;

import com.plarium.south.superflow.core.utils.AvroUtilsInternal;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.util.*;

@Slf4j
public class SchemaTransformFn extends DoFn<Row, Row> {
    private final Schema originSchema;
    private final Schema targetSchema;
    private final AvroUtilsInternal.FieldConversion conversion;

    public SchemaTransformFn(Schema originSchema, Schema targetSchema, AvroUtilsInternal.FieldConversion conversion){
        this.originSchema = originSchema;
        this.targetSchema = targetSchema;
        this.conversion = conversion;
    }

    @ProcessElement
    public void processElement(@Element Row originRow, ProcessContext context) throws Exception {
        context.output(buildRow(this.originSchema, this.targetSchema, originRow));
    }

    private Row buildRow(Schema originSchema,
                         Schema targetSchema,
                         Row originRow) throws Exception {
        Row.Builder rowBuilder = Row.withSchema(targetSchema);
        List<Schema.Field> originFields = originSchema.getFields();
        List<Schema.Field> targetFields = targetSchema.getFields();
        for (int i = 0; i < originFields.size(); i++) {
            Schema.Field originField = originFields.get(i);
            Schema.Field targetField = targetFields.get(i);

            String originFieldName = originField.getName();
            if (!originFieldName.equals(targetField.getName())){
                throw new Exception(String.format("Source's and target's schemas are not match by ordinal. " +
                        "Expected %s at position %d but was %s", originField.getName(), i, targetField.getName()));
            }

            conversion.pushFieldName(originFieldName);
            rowBuilder.addValue(convertValue(originField.getType(), targetField.getType(), originRow.getValue(i)));
            conversion.pop();
        }

        return rowBuilder.build();
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private Object convertValue(Schema.FieldType originFieldType,
                                Schema.FieldType targetFieldType,
                                Object value) throws Exception {

        Schema.TypeName originFieldTypeName = originFieldType.getTypeName();

        if (originFieldTypeName.isCollectionType()) {
            Schema.FieldType elementFieldType = originFieldType.getCollectionElementType();
            Schema.FieldType targetElementFieldType = targetFieldType.getCollectionElementType();

            if (elementFieldType.typesEqual(targetElementFieldType)){
                // Array elements are of the same type
                return value;
            }

            Collection<Object> sourceArray = (Collection<Object>) value;
            Collection<Object> targetArray = new ArrayList<>(sourceArray.size());
            for (Object arrayElement : sourceArray){
                targetArray.add(convertValue(elementFieldType, targetElementFieldType, arrayElement));
            }
            return targetArray;
        }

        if (originFieldTypeName.isCompositeType()){ // Row
            Schema sourceSchema = originFieldType.getRowSchema();
            Schema targetSchema = targetFieldType.getRowSchema();

            if (sourceSchema.equivalent(targetSchema)){
                return value;
            }

            return buildRow(sourceSchema, targetSchema, (Row)value);
        }

        if (originFieldTypeName.isMapType()){
            Schema.FieldType originMapValueType = originFieldType.getMapValueType();
            Schema.FieldType targetMapValueType = targetFieldType.getMapValueType();

            if (originFieldType.typesEqual(targetFieldType)){
                return value;
            }

            Map<String,Object> sourceMap = (Map<String,Object>)value;
            Map<String,Object> targetMap = new HashMap<>();

            for (String mapKey : sourceMap.keySet()){
                targetMap.put(mapKey,
                        convertValue(originMapValueType, targetMapValueType, sourceMap.get(mapKey)));
            }

            return targetMap;
        }

        if (originFieldTypeName.isLogicalType()){ // Enums
            AvroUtilsInternal.EnumConversionStrategy conversionStrategy = conversion.getFieldStrategy();

            Object result;
            EnumerationType enumerationType = (EnumerationType)originFieldType.getLogicalType();
            EnumerationType.Value enumValue = enumerationType.toInputType((int)value);
            switch (conversionStrategy) {
                case ENUM_TO_INT:
                    result = enumValue.getValue();
                    break;
                case ENUM_TO_STRING:
                    result = enumValue.toString();
                    break;
                case NO_CONVERSION:
                default:
                    result = value;
                    break;
            }

            return result;
        }

        return value;
    }
}
