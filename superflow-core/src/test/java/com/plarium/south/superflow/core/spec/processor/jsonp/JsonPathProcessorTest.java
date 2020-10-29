package com.plarium.south.superflow.core.spec.processor.jsonp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.commons.jsonpath.operator.*;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class JsonPathProcessorTest {

    private final String processorName = "proc-name";
    private String inputTag = "input-tag";
    private String outputTag = "output-tag";
    private String select = "$.['Uuid', 'UserId', 'SessionId', 'Products', 'Timestamp']";
    private String pipeCopyKey1 = "Uuid";
    private String pipeCopyFrom1 = "$.Uuid";
    private String pipeCopyTo1 = "$.Products[*]";
    private String pipeRenamePath = "$";
    private String pipeRenameOld = "Uuid";
    private String pipeRenameNew = "uuid";
    private String pipeDelPath = "$.SessionId";
    private String pipeConstKey = "CONST";
    private String pipeConstPath = "$";
    private String pipeConstVal = "10";
    private String pipeSelect = "$.Products[*]";
    private String ignoreEmpty = "false";
    private String outputSchema = "output-avro-schema";


    private Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("processor.name", processorName);
        env.put("input.tag", inputTag);
        env.put("output.tag", outputTag);
        env.put("ignore.empty", ignoreEmpty);
        env.put("select", select);

        env.put("pipe.copy.key1", pipeCopyKey1);
        env.put("pipe.copy.from1", pipeCopyFrom1);
        env.put("pipe.copy.to1", pipeCopyTo1);

        env.put("pipe.rename.path", pipeRenamePath);
        env.put("pipe.rename.old", pipeRenameOld);
        env.put("pipe.rename.new", pipeRenameNew);

        env.put("pipe.del.path", pipeDelPath);

        env.put("pipe.const.key", pipeConstKey);
        env.put("pipe.const.path", pipeConstPath);
        env.put("pipe.const.val", pipeConstVal);

        env.put("pipe.select", pipeSelect);
        env.put("output.schema", outputSchema);
        return env;
    }

    @Test
    public void testSpecYamlParsing() throws IOException {
        String yamlTemplated = TestSpecUtils.renderTemplate("/templates/processor/jsonp/jsonp-processor.yaml", getEnv());
        TypeReference<JsonPathProcessor> type = new TypeReference<JsonPathProcessor>(){};
        List<JsonPathProcessor> processors = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (JsonPathProcessor processor : processors) {
            processor.setup();

            assertEquals(processor.getType(), JsonPathProcessor.TYPE);
            assertEquals(processor.getName(), processorName);
            assertEquals(processor.getInputTag(), inputTag);
            assertEquals(processor.getOutputTag(), outputTag);
            assertEquals(processor.getSelect(), select);
            assertEquals(processor.getOutputSchema(), outputSchema);
            assertEquals(processor.getIgnoreEmpty(), Boolean.valueOf(ignoreEmpty));

            List<JsonPathOperator> pipeline = processor.getPipeline();
            assertTrue(pipeline != null && pipeline.size() == 5);

            CopyJsonPathOperator copyOp = (CopyJsonPathOperator) pipeline.get(0);
            assertTrue(copyOp.getOps() != null && !copyOp.getOps().isEmpty());
            CopyJsonPathOperator.Mapping copyMap = copyOp.getOps().get(0);
            assertEquals(copyMap.getKey(), pipeCopyKey1);
            assertEquals(copyMap.getFrom(), pipeCopyFrom1);
            assertEquals(copyMap.getTo(), pipeCopyTo1);

            RenamePathOperator renameOp = (RenamePathOperator) pipeline.get(1);
            assertTrue(renameOp.getOps() != null && !copyOp.getOps().isEmpty());
            RenamePathOperator.Mapping renameMap = renameOp.getOps().get(0);
            assertEquals(renameMap.getPath(), pipeRenamePath);
            assertEquals(renameMap.getOld(), pipeRenameOld);
            assertEquals(renameMap.getNewName(), pipeRenameNew);

            DeleteJsonPathOperator delOp = (DeleteJsonPathOperator) pipeline.get(2);
            assertTrue(delOp.getOps() != null && !copyOp.getOps().isEmpty());
            String delPath = delOp.getOps().get(0);
            assertEquals(delPath, pipeDelPath);

            AddConstPathOperator constOp = (AddConstPathOperator) pipeline.get(3);
            assertTrue(constOp.getOps() != null && !copyOp.getOps().isEmpty());
            AddConstPathOperator.Mapping constMap = constOp.getOps().get(0);
            assertEquals(constMap.getKey(), pipeConstKey);
            assertEquals(constMap.getPath(), pipeConstPath);
            assertEquals(constMap.getVal(), pipeConstVal);

            SelectJsonPathOperator selectOp = (SelectJsonPathOperator) pipeline.get(4);
            assertEquals(selectOp.getSelect(), pipeSelect);
        }
    }

}