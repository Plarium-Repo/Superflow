package com.plarium.south.superflow.core.utils;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.sql.Date;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.sql.JDBCType.valueOf;

public class JdbcSchemaUtil {

    private JdbcSchemaUtil() {}

    @FunctionalInterface
    public interface ResultSetFieldExtractor extends Serializable {
        Object extract(ResultSet rs, Integer index) throws SQLException;
    }

    private static final EnumMap<Schema.TypeName, JdbcSchemaUtil.ResultSetFieldExtractor>
            RESULTSET_FIELD_EXTRACTORS =
            new EnumMap<>(
                    ImmutableMap.<Schema.TypeName, JdbcSchemaUtil.ResultSetFieldExtractor>builder()
                            .put(Schema.TypeName.BOOLEAN, ResultSet::getBoolean)
                            .put(Schema.TypeName.BYTE, ResultSet::getByte)
                            .put(Schema.TypeName.BYTES, ResultSet::getBytes)
                            .put(Schema.TypeName.DATETIME, ResultSet::getTimestamp)
                            .put(Schema.TypeName.DECIMAL, ResultSet::getBigDecimal)
                            .put(Schema.TypeName.DOUBLE, ResultSet::getDouble)
                            .put(Schema.TypeName.FLOAT, ResultSet::getFloat)
                            .put(Schema.TypeName.INT16, ResultSet::getShort)
                            .put(Schema.TypeName.INT32, ResultSet::getInt)
                            .put(Schema.TypeName.INT64, ResultSet::getLong)
                            .put(Schema.TypeName.STRING, ResultSet::getString)
                            .build());

    private static final JdbcSchemaUtil.ResultSetFieldExtractor DATE_EXTRACTOR = createDateExtractor();
    private static final JdbcSchemaUtil.ResultSetFieldExtractor TIME_EXTRACTOR = createTimeExtractor();
    private static final JdbcSchemaUtil.ResultSetFieldExtractor TIMESTAMP_EXTRACTOR = createTimestampExtractor();

    /**
     * Interface implemented by functions that create Beam {@link
     * org.apache.beam.sdk.schemas.Schema.Field} corresponding to JDBC field metadata.
     */
    @FunctionalInterface
    interface BeamFieldConverter extends Serializable {
        Schema.Field create(int index, ResultSetMetaData md) throws SQLException;
    }

    private static JdbcSchemaUtil.BeamFieldConverter jdbcTypeToBeamFieldConverter(JDBCType jdbcType) {
        switch (jdbcType) {
            case ARRAY:
                return beamArrayField();
            case BIGINT:
                return beamFieldOfType(Schema.FieldType.INT64);
            case BINARY:
            case LONGVARBINARY:
                return beamFieldOfType(Schema.FieldType.BYTES);
            case BIT:
                return beamFieldOfType(LogicalTypes.JDBC_BIT_TYPE);
            case BOOLEAN:
                return beamFieldOfType(Schema.FieldType.BOOLEAN);
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARBINARY:
            case VARCHAR:
                return beamFieldOfType(Schema.FieldType.STRING);
            case DATE:
                return beamFieldOfType(LogicalTypes.JDBC_DATE_TYPE);
            case DECIMAL:
            case NUMERIC:
                return beamFieldOfType(Schema.FieldType.DECIMAL);
            case DOUBLE:
                return beamFieldOfType(Schema.FieldType.DOUBLE);
            case FLOAT:
                return beamFieldOfType(LogicalTypes.JDBC_FLOAT_TYPE);
            case INTEGER:
                return beamFieldOfType(Schema.FieldType.INT32);
            case REAL:
                return beamFieldOfType(Schema.FieldType.FLOAT);
            case SMALLINT:
                return beamFieldOfType(Schema.FieldType.INT16);
            case TIME:
                return beamFieldOfType(LogicalTypes.JDBC_TIME_TYPE);
            case TIMESTAMP:
                return beamFieldOfType(Schema.FieldType.DATETIME);
            case TIMESTAMP_WITH_TIMEZONE:
                return beamFieldOfType(LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE);
            case TINYINT:
                return beamFieldOfType(Schema.FieldType.BYTE);
            default:
                throw new UnsupportedOperationException(
                        "Converting " + jdbcType + " to Beam schema type is not supported");
        }
    }

    /** Infers the Beam {@link Schema} from {@link ResultSetMetaData}. */
    public static Schema toBeamSchema(ResultSetMetaData md) throws SQLException {
        Schema.Builder schemaBuilder = Schema.builder();

        for (int i = 1; i <= md.getColumnCount(); i++) {
            JDBCType jdbcType = valueOf(md.getColumnType(i));
            JdbcSchemaUtil.BeamFieldConverter fieldConverter = jdbcTypeToBeamFieldConverter(jdbcType);
            schemaBuilder.addField(fieldConverter.create(i, md));
        }

        return schemaBuilder.build();
    }

    /** Converts a primitive JDBC field to corresponding Beam schema field. */
    private static JdbcSchemaUtil.BeamFieldConverter beamFieldOfType(Schema.FieldType fieldType) {
        return (index, md) -> {
            String label = md.getColumnLabel(index);
            return Schema.Field.of(label, fieldType)
                    .withNullable(md.isNullable(index) == ResultSetMetaData.columnNullable);
        };
    }

    /** Converts array fields. */
    private static JdbcSchemaUtil.BeamFieldConverter beamArrayField() {
        return (index, md) -> {
            JDBCType elementJdbcType = valueOf(md.getColumnTypeName(index));
            JdbcSchemaUtil.BeamFieldConverter elementFieldConverter = jdbcTypeToBeamFieldConverter(elementJdbcType);

            String label = md.getColumnLabel(index);
            Schema.FieldType elementBeamType = elementFieldConverter.create(index, md).getType();
            return Schema.Field.of(label, Schema.FieldType.array(elementBeamType))
                    .withNullable(md.isNullable(index) == ResultSetMetaData.columnNullable);
        };
    }

    /** Creates a {@link JdbcSchemaUtil.ResultSetFieldExtractor} for the given type. */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static JdbcSchemaUtil.ResultSetFieldExtractor createFieldExtractor(Schema.FieldType fieldType) {
        Schema.TypeName typeName = fieldType.getTypeName();
        switch (typeName) {
            case ARRAY:
            case ITERABLE:
                Schema.FieldType elementType = fieldType.getCollectionElementType();
                JdbcSchemaUtil.ResultSetFieldExtractor elementExtractor = createFieldExtractor(elementType);
                return createArrayExtractor(elementExtractor);
            case DATETIME:
                return TIMESTAMP_EXTRACTOR;
            case LOGICAL_TYPE:
                return createLogicalTypeExtractor(fieldType.getLogicalType());
            default:
                if (!RESULTSET_FIELD_EXTRACTORS.containsKey(typeName)) {
                    throw new UnsupportedOperationException(
                            "BeamRowMapper does not have support for fields of type " + fieldType.toString());
                }
                return RESULTSET_FIELD_EXTRACTORS.get(typeName);
        }
    }

    /** Creates a {@link JdbcSchemaUtil.ResultSetFieldExtractor} for array types. */
    private static JdbcSchemaUtil.ResultSetFieldExtractor createArrayExtractor(
            JdbcSchemaUtil.ResultSetFieldExtractor elementExtractor) {
        return (rs, index) -> {
            Array arrayVal = rs.getArray(index);
            if (arrayVal == null) {
                return null;
            }

            List<Object> arrayElements = new ArrayList<>();
            ResultSet arrayRs = arrayVal.getResultSet();
            while (arrayRs.next()) {
                arrayElements.add(elementExtractor.extract(arrayRs, 1));
            }
            return arrayElements;
        };
    }

    /** Creates a {@link JdbcSchemaUtil.ResultSetFieldExtractor} for logical types. */
    @SuppressWarnings("unchecked")
    private static <InputT, BaseT> JdbcSchemaUtil.ResultSetFieldExtractor createLogicalTypeExtractor(
            final Schema.LogicalType<InputT, BaseT> fieldType) {
        String logicalTypeName = fieldType.getIdentifier();
        JDBCType underlyingType = JDBCType.valueOf(logicalTypeName);
        switch (underlyingType) {
            case DATE:
                return DATE_EXTRACTOR;
            case TIME:
                return TIME_EXTRACTOR;
            case TIMESTAMP_WITH_TIMEZONE:
                return TIMESTAMP_EXTRACTOR;
            default:
                JdbcSchemaUtil.ResultSetFieldExtractor extractor = createFieldExtractor(fieldType.getBaseType());
                return (rs, index) -> fieldType.toInputType((BaseT) extractor.extract(rs, index));
        }
    }

    /** Convert SQL date type to Beam DateTime. */
    private static JdbcSchemaUtil.ResultSetFieldExtractor createDateExtractor() {
        return (rs, i) -> {
            Date date = rs.getDate(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
            if (date == null) {
                return null;
            }
            ZonedDateTime zdt = ZonedDateTime.of(date.toLocalDate(), LocalTime.MIDNIGHT, ZoneOffset.UTC);
            return new DateTime(zdt.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
        };
    }

    /** Convert SQL time type to Beam DateTime. */
    private static JdbcSchemaUtil.ResultSetFieldExtractor createTimeExtractor() {
        return (rs, i) -> {
            Time time = rs.getTime(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
            if (time == null) {
                return null;
            }
            return new DateTime(time.getTime(), ISOChronology.getInstanceUTC())
                    .withDate(new LocalDate(0L));
        };
    }

    /** Convert SQL timestamp type to Beam DateTime. */
    private static JdbcSchemaUtil.ResultSetFieldExtractor createTimestampExtractor() {
        return (rs, i) -> {
            Timestamp ts = rs.getTimestamp(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
            if (ts == null) {
                return null;
            }
            return new DateTime(ts.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
        };
    }

    /**
     * A {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper} implementation that converts JDBC
     * results into Beam {@link Row} objects.
     */
    public static final class BeamRowMapper implements JdbcIO.RowMapper<Row> {
        private final Schema schema;
        private final List<JdbcSchemaUtil.ResultSetFieldExtractor> fieldExtractors;

        public static JdbcSchemaUtil.BeamRowMapper of(Schema schema) {
            List<JdbcSchemaUtil.ResultSetFieldExtractor> fieldExtractors =
                    IntStream.range(0, schema.getFieldCount())
                            .mapToObj(i -> createFieldExtractor(schema.getField(i).getType()))
                            .collect(Collectors.toList());

            return new JdbcSchemaUtil.BeamRowMapper(schema, fieldExtractors);
        }

        public BeamRowMapper(Schema schema, List<JdbcSchemaUtil.ResultSetFieldExtractor> fieldExtractors) {
            this.schema = schema;
            this.fieldExtractors = fieldExtractors;
        }

        @Override
        public Row mapRow(ResultSet rs) throws Exception {
            Row.Builder rowBuilder = Row.withSchema(schema);
            for (int i = 0; i < schema.getFieldCount(); i++) {
                rowBuilder.addValue(fieldExtractors.get(i).extract(rs, i + 1));
            }
            return rowBuilder.build();
        }
    }

    /**
     * compares two fields. Does not compare nullability of field types.
     *
     * @param a field 1
     * @param b field 2
     * @return TRUE if fields are equal. Otherwise FALSE
     */
    public static boolean compareSchemaField(Schema.Field a, Schema.Field b) {
        if (!a.getName().equalsIgnoreCase(b.getName())) {
            return false;
        }

        return compareSchemaFieldType(a.getType(), b.getType());
    }

    /**
     * checks nullability for fields.
     *
     * @param fields for check on nullable
     * @return TRUE if any field is not nullable
     */
    public static boolean checkNullabilityForFields(List<Schema.Field> fields) {
        return fields.stream().anyMatch(field -> !field.getType().getNullable());
    }

    /**
     * compares two FieldType. Does not compare nullability.
     *
     * @param a FieldType 1
     * @param b FieldType 2
     * @return TRUE if FieldType are equal. Otherwise FALSE
     */
    @SuppressWarnings("ConstantConditions")
    public static boolean compareSchemaFieldType(Schema.FieldType a, Schema.FieldType b) {
        if (a.getTypeName().equals(b.getTypeName())) {
            return !a.getTypeName().equals(Schema.TypeName.LOGICAL_TYPE)
                    || compareSchemaFieldType(
                    a.getLogicalType().getBaseType(), b.getLogicalType().getBaseType());
        } else if (a.getTypeName().isLogicalType()) {
            return a.getLogicalType().getBaseType().getTypeName().equals(b.getTypeName());
        } else if (b.getTypeName().isLogicalType()) {
            return b.getLogicalType().getBaseType().getTypeName().equals(a.getTypeName());
        }
        return false;
    }


    public static Schema fetchBeamSchemaFromQuery(DataSource ds, String query) {
        try (Connection conn = ds.getConnection();
             PreparedStatement st = conn.prepareStatement(query)) {
            return JdbcSchemaUtil.toBeamSchema(st.getMetaData());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
