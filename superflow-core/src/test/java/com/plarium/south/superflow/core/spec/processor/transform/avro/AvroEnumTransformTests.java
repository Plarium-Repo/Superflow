package com.plarium.south.superflow.core.spec.processor.transform.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.commons.transform.EnumFieldMapping;
import com.plarium.south.superflow.core.utils.AvroUtilsInternal;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroEnumTransformTests {
    private static final String PROC_NAME = "transform-proc-name";

    private static final String INPUT_TAG = "input-tag";
    private static final String OUTPUT_TAG = "output-tag";

    private static final String TEST_PATH_1 = "test_path_1";
    private static final AvroUtilsInternal.EnumConversionStrategy TEST_PATH_1_VALUE = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_STRING;

    private static final String TEST_PATH_2 = "test_path_2";
    private static final AvroUtilsInternal.EnumConversionStrategy TEST_PATH_2_VALUE = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_INT;

    private static final String OBJECT_ROOT_PATH_WILDCARD = "test.object.*";
    private static final String OBJECT_ROOT_PATH_WILDCARD_EXPECTED = "test.object";
    private static final AvroUtilsInternal.EnumConversionStrategy OBJECT_ROOT_PATH_WILDCARD_VALUE = AvroUtilsInternal.EnumConversionStrategy.ENUM_TO_STRING;

    @Test
    public void schemaParsingTest() throws IOException {
        String yamlTemplated = TestSpecUtils.renderTemplate("/templates/processor/transform/avro/avro-enum-transform-processor.yaml", getEnv());
        TypeReference<AvroEnumTransform> type = new TypeReference<AvroEnumTransform>() {};
        List<AvroEnumTransform> processors = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (AvroEnumTransform processor : processors) {
            processor.setup();

            assertEquals(processor.getType(), AvroEnumTransform.TYPE);
            assertEquals(processor.getName(), PROC_NAME);
            assertEquals(processor.getInputTag(), INPUT_TAG);
            assertEquals(processor.getOutputTag(), OUTPUT_TAG);

            List<EnumFieldMapping> mappings = processor.getMappings();
            assertEquals("Wrong amount of mappings parsed", 2, mappings.size());

            assertMappingCorrect(mappings.get(0), TEST_PATH_1, TEST_PATH_1_VALUE);
            assertMappingCorrect(mappings.get(1), TEST_PATH_2, TEST_PATH_2_VALUE);
        }
    }

    private void assertMappingCorrect(EnumFieldMapping mapping,
                                      String expectedPath,
                                      AvroUtilsInternal.EnumConversionStrategy expectedConversion){
        assertEquals("Wrong path parsed", expectedPath, mapping.path);
        assertEquals("Wrong conversion parsed", expectedConversion, mapping.conversionStrategy);
    }

    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("processor.name", PROC_NAME);
        env.put("input.dataset.name", INPUT_TAG);
        env.put("output.dataset.name", OUTPUT_TAG);
        env.put("transform.enum.path_1", TEST_PATH_1);
        env.put("transform.enum.path_1_conversion", TEST_PATH_1_VALUE.getType());
        env.put("transform.enum.path_2", TEST_PATH_2);
        env.put("transform.enum.path_2_conversion", TEST_PATH_2_VALUE.getType());
        return env;
    }

    @Test
    public void correctColumnMapBuildTest(){
        List<EnumFieldMapping> inputMappings = new ArrayList<>(3);
        inputMappings.add(new EnumFieldMapping(OBJECT_ROOT_PATH_WILDCARD, OBJECT_ROOT_PATH_WILDCARD_VALUE));

        AvroEnumTransform transform = new AvroEnumTransform("test_name", "input_tag", "output_tag", inputMappings);
        Map<String, AvroUtilsInternal.EnumConversionStrategy> conversionMap = transform.getColumnMappings();

        Set<String> keys = conversionMap.keySet();
        assertEquals("Wrong amount of the mappings built",1, keys.size());
        String actualKey = keys.iterator().next();
        assertEquals("For wildcards star and last dot should being removed", OBJECT_ROOT_PATH_WILDCARD_EXPECTED, actualKey);
    }
}
