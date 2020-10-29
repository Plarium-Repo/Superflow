package com.plarium.south.superflow.core;

import com.plarium.south.superflow.core.options.DataflowOptions;
import com.plarium.south.superflow.core.options.FlinkOptions;
import com.plarium.south.superflow.core.options.LocalOptions;
import com.plarium.south.superflow.core.options.SpecOptions;
import com.plarium.south.superflow.core.spec.commons.Specs;
import com.plarium.south.superflow.core.spec.flow.BaseFlow;
import com.plarium.south.superflow.core.spec.flow.EnvFlow;
import com.plarium.south.superflow.core.spec.flow.FlowOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;

import java.io.IOException;

import static com.plarium.south.superflow.core.spec.flow.EnvFlow.fromOptions;
import static com.plarium.south.superflow.core.utils.BeamOptionUtils.createOptionsNoStrict;
import static com.plarium.south.superflow.core.utils.BeamOptionUtils.createOptionsStrict;
import static com.plarium.south.superflow.core.utils.PathUtils.getPath;

@Slf4j
public class Boot {

    public static void main(String[] args) throws IOException {
        Specs specs = parseSpecs(args);

        FlowOptions flowOptions = specs
                .getSpecByType(FlowOptions.class)
                .addArgsWithReplace(args);

        PipelineOptions cmdOptions =
                createOptionsNoStrict(args, PipelineOptions.class);

        Class<? extends SpecOptions> runnerOptionsClass =
                selectRunnerOptionsClass(cmdOptions);

        PipelineOptions allOptions =
                createOptionsStrict(flowOptions.toArgs(), runnerOptionsClass);

        Pipeline pipeline =
                Pipeline.create(allOptions);

        specs.getSpecsByType(BaseFlow.class).forEach(job -> {
            job.setup();
            job.expand(pipeline, allOptions);
        });

        Duration maxAwait = fetchMaxAwait(allOptions);
        pipeline.run().waitUntilFinish(maxAwait);
    }

    private static Specs parseSpecs(String[] args) throws IOException {
        SpecOptions options = createOptionsStrict(args, SpecOptions.class);
        EnvFlow env = fromOptions(options);
        addImplicitVariables(env);
        return Specs.of(env, getPath(options.getSpec()));
    }


    private static void addImplicitVariables(EnvFlow env) {
        env.put("__FLOW_START_UTC_TS__", Long.toString(java.time.Instant.now().toEpochMilli()));
    }

    private static Class<? extends SpecOptions> selectRunnerOptionsClass(PipelineOptions options) {
        Class<? extends PipelineRunner<?>> runner = options.getRunner();

        if (runner.equals(DirectRunner.class)) {
            return LocalOptions.class;
        } else if (runner.equals(DataflowRunner.class)) {
            return DataflowOptions.class;
        } else if (runner.equals(FlinkRunner.class)) {
            return FlinkOptions.class;
        }
        else {
            throw new IllegalArgumentException("Not supported runner: " + runner);
        }
    }

    private static Duration fetchMaxAwait(PipelineOptions options) {
        long maxAwait = options.as(SpecOptions.class).getMaxAwait().longValue();
        return Duration.standardMinutes(maxAwait);
    }
}