package com.plarium.south.superflow.core.utils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldConversionTests {

    private static final String FLAT_PATH_1 = "test_flat_path_1";
    private static final AvroUtilsInternal.EnumConversionStrategy FLAT_VALUE_1 = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_INT;

    private static final String FLAT_PATH_2 = "test_flat_path_2";
    private static final AvroUtilsInternal.EnumConversionStrategy FLAT_VALUE_2 = AvroUtilsInternal.EnumConversionStrategy.NO_CONVERSION;

    private static final String FLAT_PATH_3 = "test_flat_path_3";
    private static final AvroUtilsInternal.EnumConversionStrategy FLAT_VALUE_3 = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_STRING;

    private static final AvroUtilsInternal.EnumConversionStrategy NO_CONVERSION_STRATEGY = AvroUtilsInternal.EnumConversionStrategy.NO_CONVERSION;

    private static final String OBJ_ROOT_PATH = "test";
    private static final String OBJ_FIELD_PATH = "test_obj";
    private static final AvroUtilsInternal.EnumConversionStrategy OBJ_ROOT_VALUE = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_STRING;

    private static final String OBJ_FIELD_1 = "test_path_1";
    private static final AvroUtilsInternal.EnumConversionStrategy OBJ_FIELD_1_VALUE = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_STRING;

    private static final String OBJ_FIELD_2 = "test_path_2";
    private static final AvroUtilsInternal.EnumConversionStrategy OBJ_FIELD_2_VALUE = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_INT;

    @Test
    public void defaultValueTest(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        conversionMap.put(FLAT_PATH_1, FLAT_VALUE_1);
        conversionMap.put(FLAT_PATH_2, FLAT_VALUE_2);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);
        assertEquals(String.format("%s must be returned by default", NO_CONVERSION_STRATEGY), NO_CONVERSION_STRATEGY, conversion.getFieldStrategy());
    }


    @Test
    public void flatPathTest() {
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        conversionMap.put(FLAT_PATH_1, FLAT_VALUE_1);
        conversionMap.put(FLAT_PATH_2, FLAT_VALUE_2);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);

        conversion.pushFieldName(FLAT_PATH_1);
        AvroUtilsInternal.EnumConversionStrategy strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for the field %s", FLAT_PATH_1), FLAT_VALUE_1, strategy);
    }

    @Test
    public void flatPathNotExistsTest(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        conversionMap.put(FLAT_PATH_1, FLAT_VALUE_1);
        conversionMap.put(FLAT_PATH_2, FLAT_VALUE_2);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);
        conversion.pushFieldName(FLAT_PATH_3);

        AvroUtilsInternal.EnumConversionStrategy strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for field %s which has no map", FLAT_PATH_3), NO_CONVERSION_STRATEGY, strategy);
    }

    @Test
    public void objPathTest(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        String absolutePath = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH + "." + OBJ_FIELD_1;
        conversionMap.put(absolutePath, OBJ_FIELD_1_VALUE);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);
        conversion.pushFieldName(OBJ_ROOT_PATH);
        conversion.pushFieldName(OBJ_FIELD_PATH);

        AvroUtilsInternal.EnumConversionStrategy strategy = conversion.getFieldStrategy();
        assertEquals("Root path doesn't have mapping - no conversion should be done", NO_CONVERSION_STRATEGY, strategy);

        conversion.pushFieldName(OBJ_FIELD_1);
        strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for the field %s", absolutePath), OBJ_FIELD_1_VALUE, strategy);
    }

    @Test
    public void objPathWithNoMapTest(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        String absolutePath = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH + "." + OBJ_FIELD_1;
        String absolutePath2 = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH  + "." + OBJ_FIELD_2;
        conversionMap.put(absolutePath, OBJ_FIELD_1_VALUE);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);
        conversion.pushFieldName(OBJ_ROOT_PATH);
        conversion.pushFieldName(OBJ_FIELD_PATH);

        AvroUtilsInternal.EnumConversionStrategy strategy = conversion.getFieldStrategy();
        assertEquals("Root path doesn't have mapping - no conversion should be done", NO_CONVERSION_STRATEGY, strategy);

        conversion.pushFieldName(OBJ_FIELD_2);
        strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for the field %s", absolutePath2), NO_CONVERSION_STRATEGY, strategy);
    }

    @Test
    public void objPathWithWildcardTest(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        String absolutePath = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH + "." + OBJ_FIELD_1;
        String absolutePath2 = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH + "." + OBJ_FIELD_2;
        conversionMap.put(OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH, OBJ_ROOT_VALUE);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);

        conversion.pushFieldName(OBJ_ROOT_PATH);
        conversion.pushFieldName(OBJ_FIELD_PATH);

        conversion.pushFieldName(OBJ_FIELD_1);
        AvroUtilsInternal.EnumConversionStrategy strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for the field %s", absolutePath), OBJ_ROOT_VALUE, strategy);
        conversion.pop();

        conversion.pushFieldName(OBJ_FIELD_2);
        strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for the field %s", absolutePath2), OBJ_ROOT_VALUE, strategy);
    }

    @Test
    public void popTest(){
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = new HashMap<>();
        String absolutePath = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH + "." + OBJ_FIELD_1;
        String absolutePath2 = OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH + "." + OBJ_FIELD_2;
        conversionMap.put(OBJ_ROOT_PATH + "." + OBJ_FIELD_PATH, OBJ_ROOT_VALUE);

        AvroUtilsInternal.FieldConversion conversion = new AvroUtilsInternal.FieldConversion(conversionMap);
        conversion.pop(); // This is in order to check the the pop operation will not throw any exception on empty path

        conversion.pushFieldName(OBJ_ROOT_PATH);
        conversion.pushFieldName(OBJ_FIELD_PATH);

        AvroUtilsInternal.EnumConversionStrategy strategy = conversion.getFieldStrategy();
        assertEquals(String.format("Wrong strategy returned for the field %s", absolutePath), OBJ_ROOT_VALUE, strategy);
        conversion.pop();

        strategy = conversion.getFieldStrategy();
        assertEquals(String.format("%s must be returned by default on empty path", NO_CONVERSION_STRATEGY), NO_CONVERSION_STRATEGY, strategy);
    }
}
