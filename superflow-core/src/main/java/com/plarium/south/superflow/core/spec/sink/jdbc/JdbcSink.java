package com.plarium.south.superflow.core.spec.sink.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.commons.jdbc.RowStatementPreparer;
import com.plarium.south.superflow.core.spec.commons.jdbc.StatementMapping;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.BasePTupleRowSink;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SINK;

@SchemaDefinition(
        type = SINK,
        baseTypes = "window",
        ignoreFields = "registry",
        required = {"type", "inputTag", "sqlTemplate", "dataSource", "variables"})

@JsonPropertyOrder({"type", "name", "registry", "window", "inputTag", "batchSize", "sqlTemplate", "dataSource", "variables"})
public class JdbcSink extends BasePTupleRowSink {
    public static final String TYPE = "sink/jdbc";

    private static final long BATCH_SIZE = 100;

    @Getter @NotNull
    @JsonPropertyDescription("The dataset name from input tuple")
    private final String inputTag;

    @Getter @NotNull
    @JsonPropertyDescription("The batch size for write operation")
    private final Long batchSize;

    @Getter @NotNull
    @JsonPropertyDescription("The sql template for insert statement")
    private final String sqlTemplate;

    @Getter @NotNull
    @JsonPropertyDescription("The data source config")
    private final DataSourceConfig dataSource;

    @Getter @NotNull
    @JsonPropertyDescription("The statement variables mapping for insert query")
    private final List<StatementMapping> variables;

    @JsonCreator
    public JdbcSink(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "batchSize") Long batchSize,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "createSchema") Boolean createSchema,
            @JsonProperty(value = "inputTag", required = true) String inputTag,
            @JsonProperty(value = "sqlTemplate", required = true) String sqlTemplate,
            @JsonProperty(value = "dataSource", required = true) DataSourceConfig dataSource,
            @JsonProperty(value = "variables", required = true) List<StatementMapping> variables)
    {
        super(name, registry, createSchema, window);
        this.inputTag = inputTag;
        this.variables = variables;
        this.dataSource = dataSource;
        this.sqlTemplate = sqlTemplate;
        this.batchSize = firstNonNull(batchSize, BATCH_SIZE);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PDone expand(PCollectionTuple input) {
        PCollection<Row> output = getByTag(input, inputTag);

        JdbcIO.Write<Row> writer = JdbcIO.<Row>write()
                .withBatchSize(batchSize)
                .withStatement(sqlTemplate)
                .withRetryStrategy(new JdbcIO.DefaultRetryStrategy())
                .withPreparedStatementSetter(new RowStatementPreparer(variables))
                .withDataSourceProviderFn(dataSourceProviderFn());

        output.apply(writer);
        return PDone.in(input.getPipeline());
    }

    private SerializableFunction<Void, DataSource> dataSourceProviderFn() {
        return v -> dataSource.buildDataSource();
    }
}
