package com.plarium.south.superflow.core.spec.source.updated.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.source.BasePTupleRowSource;
import com.plarium.south.superflow.core.spec.source.mapping.UpdatedJdbcSourceMapping;
import com.plarium.south.superflow.core.utils.JdbcSchemaUtil;
import com.plarium.south.superflow.core.utils.JdbcSchemaUtil.BeamRowMapper;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.dbcp2.BasicDataSource;
import org.hibernate.validator.constraints.NotEmpty;
import org.joda.time.Duration;

import javax.validation.constraints.NotNull;


import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SOURCE;

@SchemaDefinition(
        type = SOURCE,
        ignoreFields = {"window", "registry"},
        required = {"type", "dataSource", "mapping"})

@JsonPropertyOrder({"type", "sqlTemplate", "dataSource"})
public class UpdatedJdbcSource extends BasePTupleRowSource {
    public static final String TYPE = "source/updated/jdbc";


    @Getter @NotNull
    @JsonPropertyDescription("The data source configuration")
    private final DataSourceConfig dataSource;

    @Getter @NotEmpty
    @JsonPropertyDescription("The data source configuration")
    private final UpdatedJdbcSourceMappingList mapping;

    @JsonCreator
    public UpdatedJdbcSource(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "dataSource") DataSourceConfig dataSource,
            @JsonProperty(value = "mapping") UpdatedJdbcSourceMappingList mapping)
    {
        super(name, null, null);
        this.dataSource = dataSource;
        this.mapping = mapping;
    }

    @Override
    public void setup() {
        mapping.setup();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean windowIsNeed() {
        return false;
    }

    @Override
    public boolean registryIsNeed() {
        return false;
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        PCollectionTuple datasets = PCollectionTuple.empty(pipeline);

        try(BasicDataSource ds = dataSource.buildDataSource())
        {
            for (UpdatedJdbcSourceMapping map : mapping.getList()) {
                Schema schema = querySchema(map.getQuery(), ds);
                BeamRowMapper rowMapper = BeamRowMapper.of(schema);

                GenerateSequence sequence = generateSequence(map.getDurationObject());
                JdbcQueryFn queryFn = new JdbcQueryFn(map.getQuery(), dataSource, rowMapper);

                PCollection<Row> dataset = pipeline
                        .apply(sequence)
                        .apply(ParDo.of(queryFn))
                        .setCoder(RowCoder.of(schema));

                dataset = applyWindow(dataset);
                datasets = datasets.and(map.getTag(), dataset);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return datasets;
    }

    private GenerateSequence generateSequence(Duration duration) {
        return GenerateSequence.from(0).withRate(1, duration);
    }

    private Schema querySchema(String query, BasicDataSource ds) {
        return JdbcSchemaUtil.fetchBeamSchemaFromQuery(ds, query);
    }

    @Override
    protected <T> PCollection<T> applyWindow(PCollection<T> input) {
        return input.apply(Window.<T>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                .discardingFiredPanes());
    }
}
