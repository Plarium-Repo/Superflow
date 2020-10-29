package com.plarium.south.superflow.core.spec.processor.enrich.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.commons.jdbc.RowStatementPreparer;
import com.plarium.south.superflow.core.spec.commons.jdbc.StatementMapping;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.processor.enrich.BaseEnricher;
import com.plarium.south.superflow.core.utils.JdbcSchemaUtil;
import com.plarium.south.superflow.core.utils.JdbcSchemaUtil.BeamRowMapper;
import lombok.Getter;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.List;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;

@SchemaDefinition(
        type = PROCESSOR,
        ignoreFields = {"registry"},
        required = {"type", "sqlTemplate", "variables", "dataSource"})

@JsonPropertyOrder({"type", "sqlTemplate", "variables", "dataSource"})
public class JdbcEnricher extends BaseEnricher {
    public static final String TYPE = "enricher/jdbc";

    @Getter @NotNull
    @JsonPropertyDescription("The sql template for enrich query")
    private final String sqlTemplate;

    @Getter @NotNull
    @JsonPropertyDescription("The prepared statement for query with origin mapping row")
    private final List<StatementMapping> variables;

    @Getter @NotNull
    @JsonPropertyDescription("The data source configuration")
    private final DataSourceConfig dataSource;


    @JsonCreator
    public JdbcEnricher(
            @JsonProperty(value = "sqlTemplate", required = true) String sqlTemplate,
            @JsonProperty(value = "variables", required = true) List<StatementMapping> variables,
            @JsonProperty(value = "dataSource", required = true) DataSourceConfig dataSource)
    {
        this.variables = variables;
        this.dataSource = dataSource;
        this.sqlTemplate = sqlTemplate;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PCollection<Row> expand(Schema mergeSchema, PCollection<Row> input) {
        return input
                .apply(dynamicJdbcReadFn(mergeSchema, querySchema()))
                .setCoder(RowCoder.of(mergeSchema));
    }

    @Override
    public Schema querySchema() {
        try(BasicDataSource ds = dataSource.buildDataSource()) {
            return JdbcSchemaUtil.fetchBeamSchemaFromQuery(ds, sqlTemplate);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ParDo.SingleOutput<Row, Row> dynamicJdbcReadFn(Schema mergeSchema, Schema sqlBeamSchema) {
        BeamRowMapper jdbcRowMapper = BeamRowMapper.of(sqlBeamSchema);
        RowStatementPreparer statement = new RowStatementPreparer(variables);
        return ParDo.of(new ReadJdbcFn(sqlTemplate, mergeSchema, dataSource, jdbcRowMapper, statement));
    }
}
