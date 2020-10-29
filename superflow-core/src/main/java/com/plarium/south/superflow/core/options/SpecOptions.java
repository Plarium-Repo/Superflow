package com.plarium.south.superflow.core.options;

import com.google.common.collect.Maps;
import org.apache.beam.sdk.options.*;

import java.util.Map;

public interface SpecOptions extends PipelineOptions {

    @Description("The file path with yaml env")
    String getEnv();
    void setEnv(String value);

    @Description("The file path with yaml spec")
    @Validation.Required
    String getSpec();
    void setSpec(String value);

    @Description("Print output dataset without save result")
    @Default.Boolean(true)
    Boolean getDryRun();
    void setDryRun(Boolean value);

    @Description("The maximum seconds to wait for the pipeline to finish after process (default 5 sec)")
    @Default.Integer(5)
    Integer getMaxAwait();
    void setMaxAwait(Integer value);

    @Description("Print output dataset without save result")
    @Default.InstanceFactory(DefaultMapEnv.class)
    Map<String, String> getEnvMap();
    void setEnvMap(Map<String, String> env);


    class DefaultMapEnv implements DefaultValueFactory<Map<String, String>> {
        @Override
        public Map<String, String> create(PipelineOptions options) {
            return Maps.newHashMap();
        }
    }
}
