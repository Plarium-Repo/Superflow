package com.plarium.south.superflow.core.spec.commons.jsonpath.operator;

import com.google.common.collect.Lists;
import com.jayway.jsonpath.DocumentContext;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.commons.jsonpath.JsonPathPipeline;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JsonPathPipelineTest {


    @Test
    public void testSetOnlySelectField() throws IOException {
        String json = TestSpecUtils.getResourceAsUTF8String("/jsonpath/standard.json");

        JsonPathPipeline pipeline = new JsonPathPipeline.Builder()
                .select("$.store.book[*]['category', 'author', 'title', 'isbn', 'price']")
                .build();

        DocumentContext doc = pipeline.process(json);
        String processed = doc.jsonString();
        Assert.assertTrue(processed.contains("category"));
        Assert.assertTrue(processed.contains("author"));
        Assert.assertTrue(processed.contains("title"));
        Assert.assertTrue(processed.contains("isbn"));
        Assert.assertTrue(processed.contains("price"));
    }

    @Test
    public void testSetOnlyOpsField() throws IOException {
        String json = TestSpecUtils.getResourceAsUTF8String("/jsonpath/standard.json");
        String expression = "$.store.book[*]['category', 'author', 'title', 'isbn', 'price']";
        SelectJsonPathOperator selectOps = new SelectJsonPathOperator("select", expression);

        JsonPathPipeline pipeline = new JsonPathPipeline.Builder()
                .operators(Lists.newArrayList(selectOps))
                .build();

        String processed = pipeline.process(json).jsonString();
        Assert.assertTrue(processed.contains("category"));
        Assert.assertTrue(processed.contains("author"));
        Assert.assertTrue(processed.contains("title"));
        Assert.assertTrue(processed.contains("isbn"));
        Assert.assertTrue(processed.contains("price"));
    }
}